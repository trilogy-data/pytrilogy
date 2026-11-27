# Handoff: the next formalization round

`formal/` holds a dependency-free Lean 4 model of Trilogy's semantics. A round
picks one area, models the documented behaviour, proves the laws, and writes a
counterexample theorem for every place the engine broke them. A Python law
suite then holds the implementation to the theorems.

History lives elsewhere, so this file stays short:

- `formal/README.md`: the module table and, under "What it found", every bug
  found, with its theorems. Read the table; search the rest when a lead looks
  familiar.
- `git log` and the PRs: what each round measured.
- This file: how to run a round, what the rounds taught, and the leads left.

**State at 0.3.544.** Areas 0-12 below each have a module and a law suite. The
`first` and `spellings` oracle families were swept to seed 450,000, the
`expressions` family to 800,000, `compositions` to 260,000, `windows` to
60,000 and `filters` (a `?` as a select target: alone, beside keys, as a
group key of aggregates and windows, in a HAVING) to 260,000; the PR of each
round holds its sweep totals. `filters` has 12 `OPEN` seeds (area 4b),
`windows` 14 (area 4); `expressions` 8628 raises on the summary model on main
(area 1).

The `Oracle sweep` workflow re-sweeps the `first` and `spellings` range on
main every night (`.github/workflows/oracle_sweep.yml`; the `expressions`
family is not in its matrix yet), since the law suites replay only the
first few hundred seeds and the ones a round found. When a round extends the
range, raise `SWEPT_TO` there and the number above. A red night names its
seeds: `python -m local_scripts.oracle_sweep show <seed>` reproduces one.

**Not in scope: ROLLUP, CUBE and GROUPING SETS.** They will be rewritten as
table functions, so do not formalize them, build an oracle family for them,
or fix their planning beyond keeping existing tests green.

## The recipe

1. **Probe before modelling.** A scratchpad script runs the same Trilogy text
   on two engines (`Dialects.X.default_executor(environment=Environment())`,
   then `execute_text(...)[-1].fetchall()`), or pairs two spellings the docs
   say are equal. Disagreements are the leads. For the planner, sweep
   `--ref origin/main` over a new seed range first: it lists the leads and
   fills main's cache for the A/B.
2. **Model in Lean** under `formal/Trilogy/<Area>.lean`, imported from
   `formal/Trilogy.lean`: the reference semantics (small, over `Int`/`List`;
   no Mathlib, so fractions are structs, see `Functions.Frac`), the laws a
   user relies on (`*_exact`, `*_add`, `*_iff`), and a named counterexample per
   bug, closed by `decide`.
3. **Python law suite** in `tests/<area>/test_<area>_laws.py`, its docstring
   naming the Lean file: seeded random data, the reference computed in Python
   with the Lean model's definitions, an assert per row.
4. **A/B against main.** For the oracle families:
   `python -m local_scripts.oracle_sweep ab --seeds <range>`. For any other
   suite, run the branch's suite against main's package:

   ```bash
   git archive origin/main trilogy | tar -x -C <scratch>
   cd <scratch> && PYTHONPATH=<scratch> <repo>/.venv/bin/python -m pytest \
     --import-mode=append <repo>/tests/<area>/test_<area>_laws.py -q
   ```

   `--import-mode=append` is required: `tests/` is a package, so pytest
   otherwise puts the repo first and the branch's `trilogy/` wins silently.
   Print `trilogy.__file__` once in every A/B, probes included. Report "N of M
   fail on main" in the PR. Never `git stash` or `git checkout`: parallel
   agents and running sweeps share the tree.
5. **Fix, ablate, document.**
   - Remove each change in turn (and guards in pairs) and rerun the suite and
     the family. Drop what nothing needs.
   - Every fixed bug gets a regression test outside the law suites: a reduced
     shape with hand-checked rows in a plain test file
     (`tests/discovery/test_sweep_<range>_shapes.py` or the area's module).
     The build jobs, where coverage comes from, do not run `*_laws.py`.
   - Add the module row and a "What it found" bullet citing the theorems to
     `formal/README.md`. Bump `trilogy/__init__.py`. Run `ruff check . --fix`,
     `black .`, `mypy trilogy`. Commit the TPC timing logs and charts the
     suites regenerate.
   - Leave this file shorter than you found it: move what is done to
     `formal/README.md`, delete leads that no longer reproduce, and add a
     lesson only when no existing one covers it.

## Tooling and what it costs

| Loop | Command | Cost (4 cores) |
|---|---|---|
| Lean | `cd formal && ~/.elan/bin/lake build` | 50 s clean, under 1 s for an untouched module |
| One law suite | `.venv/bin/python -m pytest tests/<area>/test_<area>_laws.py -q` | seconds to minutes |
| Oracle sweep | `python -m local_scripts.oracle_sweep ab --seeds A:B` | ~7 ms a query; main's side once per commit |
| One seed | `python -m local_scripts.oracle_sweep show <seed> [--ref origin/main]` | 2 s |

- **Lean.** `lake` is not on PATH; the toolchain is in `~/.elan` (`elan show`
  reports no default, the build still runs). Keep the build warning-free:
  `if_pos`/`if_neg` are deprecated (`simp only [h, ↓reduceIte]`), and `omega`
  handles `%` by literals. Close a bounded-domain proof that needs
  `maxRecDepth` with `decide +kernel`: plain `decide` evaluates the
  proposition in the elaborator and again in the kernel, and those few proofs
  were 90% of the build. It adds no axiom (`native_decide` would).
- **Sweep.** `local_scripts/oracle_sweep/README.md` has the commands. Records
  are cached per commit for a ref and per state of `trilogy/` for the working
  tree, and are written as each chunk finishes, so a killed run resumes and a
  rerun on unchanged sources is free. Iterate on a slice (`--seeds @file`
  replays the seeds a diff named), then confirm on the whole range. Run
  `--carry-state` once a round: the default plans each seed on a clean model
  and cannot see a bug that needs an earlier statement's definition. A
  family's law file is part of its cache key: edit `OPEN`/`FOUND_SEEDS` after
  the last sweep, or copy the ref's records to the new key.
- **Sweep limits.** A background command stops at 2 hours and a container can
  restart: rerun the same command and it continues. One sweep at a time; a
  second on the same records refuses to start. Never start one with a
  trailing `&` in a foreground command, and never move the worktree under it.
- **pytest.** One pytest invocation at a time on a shared tree. Every
  `*_laws.py` passes under `-n 4` (31,660 cases, 9.5 minutes against 28 of
  CPU), and CI's `laws` job runs them `-n auto`. A suite that defines concepts
  needs a fresh environment per case, or its failures move with `-n`.
- **Other harnesses.** The fuzzer (`python -m local_scripts.fuzzer`; adding a
  spec changes `CASES_PER_SEED` in `local_scripts/fuzzer/test_fuzzer.py`, which
  CI does not run). The TPC-DS corpus at sf=0.1 ablates in ~20 minutes. The
  rollup matrix is `tests/discovery/test_rollup_resolution_laws.py`: extend
  it, do not start another.

## Lessons

### Finding leads

- Probe a long list of literal expressions on two engines at once, then the
  same expressions over a column: a literal tests the parser's fold, a column
  the dialect's rendering. Read the SQL of every literal that disagrees.
- Probe the docs' invariants, not only engine pairs: "a concept has one value
  whatever else is projected", "a group's answer is the answer filtered to
  it", `P or false` is `P`, a filter never adds rows.
- Pair a value with its twin: five spellings of one value (`s`, `t <- s`,
  `t <- s + 0`, `(s + 0)`, inline), a literal beside an expression of
  literals (`1` and `1 - 10` were planned at different grains), `in (x)` beside `= x`, `x ? c` beside
  `case when c then x end`, parenthesized beside bare. A twin needs no oracle
  and points at what differs. It can also share the bug.
- Put a concept in every position: alone, beside an aggregate, beside its own
  inputs, in a membership, a rowset, an aggregate filter at another grain, an
  OR with a joined dimension. Then follow it through its life: filtered,
  gated, aggregated, persisted, read back. Bugs sit one step past the
  positions the last round listed.
- A persisted copy is a datasource whose types, nullability and grain are
  claims the planner reads back. Persist every expression in a sweep, read the
  table raw, and diff every read before and after the `persist`.
- Compare a declared type with the engine's `typeof`, not only the value.
- Sweep a family, not only random seeds; a new generator finds more than
  more seeds of the last (the third failed on 1.6% of its seeds where the
  older two were down to 4 in 60,000). Generate the spellings a user writes.
- Run every suite on every engine CI has (DuckDB, SQLite, chdb), and render
  every dialect: the ones with no engine in CI break in the rendering.
- A sweep with no wrong rows still has a round in it: group raises by message
  and reduce the largest group. A family whose oracle raises is worth
  reading. The pass-cap warning in a sweep log is a fixpoint bug.
- Mine cached records for efficiency leads (queries holding only summable
  aggregates that still scan the base table), not only for wrong rows.
- Re-test a handoff's prediction before building on it.
- Read the generator's docstring before deciding an oracle: the rule for a
  selected `?` was written in `gen_filter`, and the leads were the positions
  that broke it. Where the code gives two readings, restrict the family to
  the inputs every reading agrees on and write the split up as a decision.
- A sweep that stops advancing is a planner loop: the runner now ends a plan
  at 30 s. Pool workers survive a killed sweep and keep a core each.
- `ab`'s `broken` lists every status that moved to a bad one, a raise that
  became wrong rows included: count main-ok seeds apart.

### Oracles and data

- Check against an independent reference (Python over the raw rows, or SQL
  written by hand), never only against another plan: rule-on and rule-off,
  summary and base, a live value and its persisted copy all share bugs.
- State the oracle from the language rule, not from the fix. Know the rule
  first: `not` is two-valued, a WHERE aggregate reads every row, an aggregate
  with no `by` is at the select's grain, an aggregate in a select target's `?`
  reads the WHERE-kept rows. An existing suite's oracle can encode the bug.
- Define a reference by a grammar or a decision, not by an engine. Where
  engines split, decide once, render it everywhere, and hold each engine to
  Python. Run the modeling suites (TPC-DS) before fixing the grammar.
- Make the data honest for the model's claims (a complete binding, a plain
  merge, a `?` enrichment). Padding over data that breaks a claim is not a
  bug.
- Give a data-driven matrix data that comes close to its claims: patterns
  derived from each row's own value, binary-exact ties on SQLite.
- Raw SQL in a fixture is part of the harness's portability. A SQLite fixture
  rolls back after one failing statement; chdb is one session per process and
  prints `inf`/`nan` as `None`.
- A strict xfail hides a crash as well as a miss: hold every open shape to
  its answer in its own test.

### Reducing and diagnosing

- Reduce until the bug needs every remaining ingredient: drop select items,
  then HAVING atoms, then rewrite the WHERE (`not (x = 0)` and `x != 0`
  differ on NULL). Ask which ingredient names the code.
- Reduce from the reporter's own model, not from the ingredients they list.
  Then test a generator over those ingredients, not the repro alone.
- When a select returns keys the WHERE rejected, read in this order: the
  cover (`elect_cover`'s winners per concept), the join plan
  (`plan_final_join`: each contributor's identity and carries, each pairing's
  keys and kind), then the join types after each narrowing step
  (`_cap_lookup_joins` to `narrow_keyless_joins`). Patch a print in from the
  scratchpad.
- "Fails only in a batch" is not a state leak until proven: bisect the prefix
  to one statement, list what it added to `environment.concepts`, and declare
  the culprit in a fresh model. A select defines its outputs by design.
- To find an optimizer pass, turn each `CONFIG.optimizations` flag off in
  turn, then log the optimizer at INFO. Count a rule's firings by its real
  return value (`optimize` returns a tuple).
- Try the generator's other spelling of a shape before reading the planner:
  "raises on even seeds" was the parser.
- Read rendered SQL for precedence before blaming the plan.
- Profile the harness before the planner.

### Fixing

- Match what was asked; never rewrite a query into what a source serves. A
  rewrite decides the plan before the search can price it.
- State a requirement as an obligation the search prices, not as a pinned
  path that runs first: a pinned path wins by position, and when it declines
  it hands its request to a search with none of its guarantees. Ablate a pin
  (return None) and diff to see what it hides.
- Fix where a value is consumed, not where it is offered; fix missing keys at
  demand, not by late widening.
- A guard must read the node actually built, not a claim about it (a bucket
  label, the group graph's `carries`, a grain a node says it has).
- A guard written for one repro needs a census: narrow each new preference to
  the atoms that can hold on an absent group, and to the argument the bug
  reads. "Holds on absent" is per argument; a COUNT reads 0 there, not NULL.
- The planner reduces grains (`species, tree_form` is `species`), so a guard
  reads the selected concepts, not `BuildGrain.from_concepts`. A rename is
  the grain it names everywhere grains are compared.
- A concept's `grain` is not always an FD; its `keys` say what determines it.
- Turning an error into rows is not progress until the rows are checked. A
  fix that unblocks a shape owes its rows; diff the wrong-answer set against
  main, not the count.
- A/B a new family against main before calling a fix done: `broken` lists
  what the fix took from main (five of this round's changes each broke seeds
  no committed case covered).
- Rerun the whole family and the committed seeds after every planner change,
  narrowing included, and the broad suite after the last one. A spot check
  of the seeds a fix targets is no A/B: 0.3.531's second batch passed every
  targeted seed and regressed 14 windows seeds, 83 filters seeds and two
  pinned plans, on both models where the checks ran one. Re-sweep the
  old range on the branch too: twice a round reported a range clean from
  records that predated a merge.
- An ablation that writes artifacts can poison the next (a failing TPC-DS
  size check restores the committed log): commit the rebaseline first.
- Read a size-budget overrun or a newly failing conformance test as a claim
  to examine, not a number to rebaseline. Read the SQL a shape test pins
  before changing it: it may pin the wrong answer's plan.
- A refusal can be load-bearing, and a validation that rejects a shape hides
  the plans behind it: probe the explicit spelling of what a rewrite outputs.
- Helper names collide in `strategy_builder.py`; grep before naming.
- New obligation kinds go into Rust too (`network_search.rs`, its `Kind`
  rank, and the parity cases of `test_v4_network_search.py`).

### Planner facts that keep recurring

- A join type has two authors: `get_join_type`, then
  `ensure_content_preservation` widens, then narrowing runs. A join remembers
  its `own_type`.
- A side pairs only on its planned keys. A column it merely exposes joins
  nothing, and column overlap is not a pairing: decide joins from the plan
  they are resolved against. A pairing the plan adds must be a kind
  `elect_bridges` accepts (a BLEND bridge is never elected).
- "Upstream of" is not "filters". Ask which group's rows a consumer
  aggregates and whether those rows passed a host. A value read past a host
  is the kept groups' value; an atom that holds on NULL turns padding into a
  pass.
- A one-row group joined on nothing is INNER. A `by *` total only the WHERE
  reads is a one-row side, not a total of the select. Only a written `by *`
  is a constant of the query.
- A lookup a host's grain determines adds no tuple, so it counts as covered.
  A nullable side is not the fact by being nullable.
- A partial is not a key: join a re-sum on the group key. Ask whether an
  aggregate is a partial at the node's grain (`pending_rollups`) before
  re-summing it.
- A window's keys stand in for it as a join key, never as a group key. Once a
  value depends on the select grain, its keys are the select grain.
- The group graph decides where a WHERE lands before source planning: mark a
  rolled root only with what the plan downstream will filter.
- A mixed grain (`by city, tree_form` beside `city, species`) is not a
  lookup; only pure-property grains take the property-rollup path.
- The all-rows marker in an identity is no key: read
  `pairing_keys(identity)` wherever an identity is compared with keys.
- A plan may say a side can be widened to a key only when a row of it reaches
  that key: a BASIC over grouped rows alone holds what they output.
- A value over a partial is computed after the partial is re-summed, and a
  filter finer than a re-sum is applied below it.
- A partial is what a source stores (`rollup_concepts`), never a finished
  value that sits beside finer rows.
- A value computed at a group is nullable where its group can make it NULL:
  any aggregate but a count over a nullable argument, and what propagates it
  (`GroupNode`, `null_on_null_group`). A nullability claim decides `=`
  against `is not distinct from`, so a missing one drops rows. Sample
  `stddev`/`var` of a one-row group is NULL over non-null values and is not
  claimed yet (no non-null numeric column in the sweep model shows it).
- A condition-phase group (`root_d1`, `d1`) holds rows the WHERE never
  filtered: rank it last wherever groups are chosen by a tie on name, and a
  regraft off a constraint-fed root drops the gate.
- A join type and the side a gate is read off are one decision: reading a
  gate's argument off the side it gated is exact only when that side is
  joined INNER. Fix the type, not the read (0.3.515's first fix moved the
  read and broke a seed that was INNER already).
- A set feeds no rows. A membership's defining group holds the rows its own
  condition kept: it is no bridge, no producer of a FINAL argument, and no
  column is carried across its existence edge (0.3.524 fixed one of each);
  nor a supplier of a select output, a regraft parent, or the grain of the
  rows it filters (0.3.528, for a set in a select target's `?`, which carries
  no `@condition` label to exclude it by).
- Where a set is written decides what it reads: in the WHERE, every row; in
  the select or the HAVING, the rows the WHERE kept. A WHERE aggregate
  reaches no scan, so a select-phase set under one is planned standalone
  beneath the WHERE (`_CleanFeederCache.get(kept=True)`).
- A one-row side restricts no key: its filter empties the merge or leaves
  it whole, so it never stands in for a keyed sibling's WHERE.
- A window counts repeats, as an aggregate does: wherever a consumer is
  excused a join because repeats dedup away, ask whether it is a window.
  Nor is its feeder a row parent of the node it gates: a walk down a node's
  parents (grain, pins) reads `_row_source_parents`, and a set is ordered
  before its own membership's left side only (0.3.525).
- A stored partial is no finished value until it is re-aggregated at its own
  grain, wherever it is read: as a gate's value, a set's condition, a key a
  merge pairs on, or a total beside keys (0.3.525 fixed one of each). Nor is
  a stored value a column of a host over another stored value, nor what a
  merged aggregate groups by a partial of its parent (0.3.526).
- `existence_arguments` of a literal `in (...)` is `[()]`, truthy: test with
  `any(...)`. Three sites in `condition_placement.py` still test truthiness.
- A filtered column (`x ? c`) and a set claim the grain of the select they
  were built in; their rows are the ones they filter. Read `keys`, the
  contract's asks or the node, never that grain. That grain names the
  filtered column the set gates, and an aggregate grouped by a filtered
  column is no partial of one its condition reads: both are cycles.
- A filtered key is NULL on the rows it rejects, so it identifies no row
  unless a parent applied its condition; nor is the column it was read off a
  key of anything downstream.
- "A barrier consumed the WHERE" holds only for a barrier the statement's
  rows flow through: one feeding a set's condition alone leaves them whole.
- A WHERE is applied once: a rewrite that restates it re-reads its sets and
  aggregates off the rows it kept. An optimizer rule that moves or folds a
  membership checks the CTE it lands in is wired to that set.
- Efficiency: "summary reads lost" is a cost only where the base scans grew.
  Attribute cost per change and per lost seed, and gate a planner-wide
  efficiency change with a full A/B before narrowing it.

### Dialects

- A dialect default is a semantic choice (`NULL_SORT`, `greatest` over NULLs,
  `x / 0`, `float` width). Pin the documented default of engines not in CI
  and simulate each rendering against it.
- Every path that renders must go through the dialect (the file-target
  `order by` did not). A rendering fix can trip a pass that reads rendered
  SQL (colon escaping, ClickHouse `{name:Type}`).
- A dialect's answer feeds planner claims: totality (`/`), nullability,
  membership (`_null_free_set`). A new NULL source changes both the persisted
  copy and `x in S`; test with a NULL probe.
- ClickHouse types are NOT NULL by default: diff a left join's padded row
  first. One statement setting covers its CTEs. A base type name can already
  be parametric (`Decimal64(4)`).
- Time a new dialect's share of a suite before adding it to CI (`seeds_for`
  runs chdb on the first seed only).

### CI

- `codecov/patch` goes red on code for dialects with no engine in CI: pin the
  rendering in `tests/dialect/`, as `test_function_law_renderings.py` does.
- ClickHouse runs through chdb on Linux only
  (`tests/engine/_clickhouse_cases.py`).
- `tests/scripts/test_cli_consistency.py::test_ingest_output_passes_integration`
  validates TPC-DS against the `std` types; tightening one breaks there.
- `test_volatile_operand_drawn_once[SQLITE]` is unseeded and can fail on a
  rare draw (a 17-digit float text); a `Writings.lean` lead.
- The Lean job restores `formal/.lake`, so it rebuilds only what a PR touched.

## Leads

Each area is done to the extent its module and suite say; these are what is
left. Leads older than 0.3.500 in area 1 were pruned: the three re-probed for
this edit no longer reproduced, so probe any lead before working on it.

### 1. Answering from intermediate aggregates (`Rollup.lean`, `ValueSet.lean`, the sweeps)

Open in the `expressions` family (each fails on main too):

- An aggregate group's merge takes no keying bridge (it would count the
  repeats), so a blend among an aggregate's own inputs is still unbridged.
- The expressions family past 800,000 is unswept: 650,000-800,000 held no
  wrong or raising seed on main.
- Efficiency: `MergeTwinScans` reads a scan under a WHERE off an unfiltered
  twin of its table. Left (about 3,300 of 300,000 queries still scan twice):
  a filtered row scan beside a grouped, unfiltered one with no row twin
  (`select tree_id where city = 'A' and count(tree_id) by species > 1`), and
  a summary re-sum beside its row twin (it reads stored partials through
  `partial_sources`, which the rebase would have to carry).

- `expressions` 8628 raises on the summary model, on main too (found by an
  A/B of 0-10,000; the family's nightly sweep is not in the matrix).

Open in the `compositions` family: none of seeds 0-260,000 is wrong or
raises. It is nearly spent (4 raising queries in 120,000 past 200,000).
- Efficiency: a set the group graph builds is planned a second time as a
  standalone feeder (`_CleanFeederCache`) for a dimension root hosting the
  same membership. Seed 83051, which showed it, no longer generates a
  membership; find a fresh shape first.
- An extra (bridge) side is widened only to a stored key its outputs
  determine (`join_plan._root_lookups`); a derived key (`fc`) still needs
  a pair bridge.

Reads the summary could serve:

- A stored count read beside tree rows joins the summary to the trees and
  groups back (`count(tree_id) / count(tree_id) by *` beside a mixed grain).

- `count(cls)` beside a stored `count(id) by cls` is the re-sum over the
  non-null classes (`ValueSet.count_column_from_row_count`); the planner does
  not rewrite it.
- The mixed grain (`by city, tree_form` beside `city, species`): it needs the
  row stream to hold the pairs.
- A nested aggregate whose inner grain the summary answers under an outer
  property (`select tree_form, max(max(height) by species)`); a bare
  `select min(height) by tree_form` with no keys; `select species,
  min(height) by city`, whose rows are the summary's own pairs.
- The coarse rollup under a WHERE finer than its keys (`species = 'Acer'`
  beside `by city`): the coarse bucket must scan the summary at its own grain
  with the WHERE applied, then group.
- A coarse aggregate that still scans should join the row stream's scan
  rather than open its own (spellings 2492, 242543; first 22464).
- 131836's root row stream sourced from the summary (it falls back to the
  base rows).
- An aggregate peel given the WHERE atom costs a JOIN where FINAL already
  joined it INNER to a filtered side; typing the join toward the side that
  applied the WHERE needs `SideFacts` to know which sides did.
- A WHERE total beside a value at another grain in one atom (`max(height) by
  * > sum(height) by city`) reads the base rows; so does a select total under
  a WHERE (rule 4 in `test_summary_cover_laws.py`).
- The summary-path guards (`_coarse_gate_beside_row_columns`,
  `total_gate_beside_rows`, `_unsourced_beside_dimension_gate`,
  `_dimension_gate_beside_another_grain`, `_unsourced_beside_rolled_gate`)
  are five shapes of one gap: the search that takes over when the joined plan
  declines. `_plan_joined_filter_rollup` sourcing an unstored gate from the
  base rows, instead of declining, would retire most of them.
- Declines marked in the harness: `DISTINCT_GRAIN_COLUMNS`, `MIXED_GRAINS`,
  `GROUP_VALUE_GRAINS`, `form_case`, `form_nested`.
- `collect_applied_conditions` counts a condition applied under a padded
  side; fixed where it bit (0.3.500), not in general. It also counts one
  applied below a group joined back to finer rows, and a set's subselect:
  FINAL's population test reads `row_applied_atoms` instead (0.3.539), the
  other readers (`_partner_facts`, `_rejects_padding`) do not yet.
- TPC-DS q83's RIGHT JOIN date pin (on main too).

### 1b. Cheapest source (`Cost.lean`)

- `source_dominance.dominates` and the network search's `dominated_reads`
  compare grains by component subset or binding, not the FD closure: a
  summary at `(city)` is not coarser than a row table at `(tree_id)`, and two
  rollup bindings at different grains with no arm tie. Sound, incomplete.

### 1c. Summary coverage (`Cover.lean`)

See `docs/handoff_summary_cover_next.md` for the approach.

- `_plan_finer_filter_rollup` and `_plan_relation_rollup` are pinned paths
  with their own ranking; move both to obligations as `JOINED` was.
- Under a joined condition the scan reads the summary at its full grain; a
  re-sum to the target plus the condition's keys is exact
  (`Cover.resum_then_case_exact`) and smaller.
- Relation shapes under a WHERE read the base rows: the group graph must hand
  a rolled root the WHERE its rows need, as `scan_filtered_roots` does.
- `species, ecoregion_id, sum(height)` under `merge ... into ecoregion_id`
  reads the base rows (`OPEN` in the relation suite).
- `_plan_relation_rollup` does not see a table binding the target's key.
- A summary storing `agg(x)` at exactly the target grain does not answer
  `agg(x ? c)`.
- `::unique` lists: other paths that dedup an unnest do not read the claim.
- Grouping by an unnested element dedups `(row key, element)` but
  `select ecoregion_id, tree_id` returns a pair per repeat: needs a decision.
- `siblings/scalar` under a WHERE on a stored column scans the base rows for
  keys the summary has.

### 0. Binding FDs (`Binding.lean`)

- A family-level "grain is unique model-wide" declaration (proposed, not
  built), with a validation check over the union of the arms
  (`Binding.arm_grain_alone_fails`).
- Three readers of row determinants (`BuildDatasource.row_determinants`,
  `Environment.binding_fds`, `domain_graph._row_determinants`) compute one
  set; fold them.
- `_pin_input_placements`: no shape found where the FD test and the
  declared-keys test differ.
- Leave alone: `_condition_source_concepts`, `_arm_scopes_sourcing`,
  `merge_node._existence_only` and `_stamp_determining_key_roots` read
  declared keys on purpose.
- Not a bug: a derivation over a subset-merged key pads where a COMPLETE
  binding's claim is false.

### 0b. Unnest filters (`Unnest.lean`)

- The keyless-join guard cannot tell a deduped list column from a literal
  `unnest([...])`; mark the UnnestNode's source.
- Completeness validation does not check a merge over a computed origin.
- Unnest of a fact-level list is not in the suite.

### 2. Optimizer rule soundness (`Rewrite.lean`)

- `merge_irrelevant_group_by` fires on no shape found
  (`collapse_single_parent` seems to pre-empt it). Count firings over TPC-DS;
  it may be dead. `fold_existence_into_aggregate` fires only under table
  addressing. Query-addressed tables are never inlined, so run both
  addressings.
- `union_sources` mints a mixed partition cover of different facts in TPC-DS
  `all_sales`; arms of one family should share a fact identity.
- `PredicatePushdownRemove`'s whole-condition path does not check
  `_parent_nullable_in_cte` (`Rewrite.remove_past_padding`); no shape reaches
  it yet.

### 3. Functions and types (`Functions.lean`, `Scalars.lean`, `Types.lean`)

- `random`: drawn once per reference, so `select x, cast(x as string)`
  disagrees with itself and a WHERE filters one draw and returns another; it
  is a fraction on DuckDB and an integer below `n` on SQLite. Decide what it
  means (per row, or one value per query); either way a volatile concept is
  computed once in a CTE and read thereafter.
- A decimal sum keeps its operands' precision, so a persisted sum can
  overflow (loud).
- `null::int` on ClickHouse renders an invalid cast.
- Chained casts: `t::date::datetime` reads `date::datetime` as a type with a
  trait and raises; `(t::date)::datetime` works.
- CASE arms of differing column types are rejected while `coalesce` merges.
- No engine in CI: Presto `date_part`, SQL Server `date_trunc`/`date_part`.

### 4. Windows, ORDER BY, set operators (`Windows.lean`, `SetOps.lean`, `SelectGrain.lean`)

Open in the `windows` family (`OPEN` in `test_oracle_windows_laws.py`, each
failing on main too). 12,000-60,000 held 123 failing queries on main and
holds 25, a fifth of the first 12,000's yield per seed, so 0.3.531 wrote the
`filters` family (4b) in place of sweeping on.

- A filtered key under a WHERE gate reads rows the gate never filtered:
  `select city ? species != 'Tilia' and min(height) by city <= 10 as f where
  max(height) by tree_form = sum(height) by tree_form` returns B (24973).
  `_hosts_reading_unfiltered_rows` pools only GROUP BY groups; pooling FILTER
  groups too filtered another shape twice.
- `select fc, row_number fc order by fc where max(height) by species <= 1 or
  tag = 'x'`: the atom is hosted on the window, whose parents (the derived
  key off the species dimension) hold no tree column (3018, 58115).
- One set written in the WHERE and again in the HAVING or a select target,
  under a WHERE no scan carries: `Missing source CTE` (16181, 58098),
  `Ambiguous reference` (1210), or the select-phase copy's total is unbuilt.
- A HAVING set under a gate atom with no set raises `cannot be related`
  (10318) once the HAVING semijoin stops restating the WHERE (78 queries in
  60,000 seeds). So the semijoin still restates such a WHERE, wrongly where
  the gate is not decided per whole group (51466, 54409; `where height > 2
  and count(tree_id) by species >= 3 having tag = 'y'` returns nothing).
- Wrong rows: 34305, 18040 (42711 is right at 0.3.542); summary only 40037, 43504, 47444
  (a re-sum fans out beside a window or a `by` denominator). Raises: 28981
  (11457 is right at 0.3.537).
- Two parents of one group with no pairing render as a cross join; the
  keyless-join guard does not fire (59727 before its fix).
- Efficiency: a summary no longer answers `agg(x ? k in <set>)` (301 of
  3,449 summary reads in the family, 85 queries scan the trees more). The
  refusal is in `_datasource_has_matching_rollup_aggregate`, which cannot see
  the WHERE; the defect it covers is narrow (the re-sum's CASE reads the
  membership's left column, which the scan's WHERE wrapper drops when the
  WHERE reads it too: `gen_root`'s wrapper and a second path). Keep the
  column on the scan and the refusal can go.
- Efficiency: a select-phase set under a WHERE aggregate is planned as a
  standalone feeder, re-reading what the group graph already built (+640
  CTEs over 24,000 queries). Hosting the WHERE's gate on the set's own filter
  group (a CONSTRAINT edge to it, and `main_lineage_groups` following a
  select-phase existence edge) would share the scan.
- State: on one executor, after seed 0 (`select species, tag, ..., sum(height)
  as v1, lag(sum(height)) over (...) as v2`) seed 567 re-sums a finished
  count per tree on the summary model (`(A, Acer)` reads 4 for 2). The suite
  plans each case on a fresh copy of the model; `--carry-state` over 0-4,000
  shows no other pair.
- The generator writes no frames (`rows between`), no window over a window
  and no membership whose set is a derived key.

- Decide whether to warn: a select of only a window ordered by a bare
  aggregate repeats ranks per tuple, and with no keys every row ties
  (`SelectGrain.bare_empty_grain_is_total`).
- `lag`/`lead` ordered by an aggregate with ties; frames (`rows between`);
  windows ordered by a scalar of a fixed value in every spelling.
- A table storing `sum(y) by g` at `(g, h)` is never read for `s`.
- A rowset member at a coarser grain than the rowset reads as covered by its
  grain.
- FINAL dedups group by more than the tuple (TPC-DS q23 twice, q30/q79 by
  hidden join keys).
- SQL Server resolves a select alias only as a bare ORDER BY term.
- A concept-level `union(amt, zero)` with `zero <- 0.0` raises a keyless
  join (on main too). `zero <- cast(0.0 as ...)` and `zero <- 1 - 1` plan:
  0.3.516 keeps a constant off a select grain computed from it, but a bare
  literal arm fails another way (`tests/engine/test_duckdb_union_arm_filter_leak.py`).
- `union` keeps the bag and is not in the Lean model; `coalesce`/`case` type
  merging is unchecked.

### 4b. A `?` as a select target (`Filters.lean`)

`gen_filter`'s docstring is the rule: `x ? c` is `x` where `c` holds and NULL
elsewhere, and a statement showing only filter values over one predicate is
filtered by it. The family writes only conditions the filtered value's keys
decide. Open in it: `OPEN` in `test_oracle_filters_laws.py` and, at 0.3.539, 553
failing queries of 0-160,000 (123 wrong, 402 raises, 28 invalid),
each failing on main too but for 25496, main's stray WHERE row beside a
window that a limit hid (0.3.538 changed the row order). In 140,000-160,000
the wrong answers left are the derived key off a peel (141488, 146267),
windows over filtered keys (141534, 148111, 155908) and 145514, 153350,
156836, unread. 129123 in full is wrong on main and here: the filter
cannot expose its grain keys (species, tag), so FINAL pairs on `v0` alone.
The renderer collapses an aggregate to its argument in a CTE finer than its
grain (`AGGREGATE_GRAIN_MATCH_MAP`); a planner placing a keyed aggregate
there returns wrong rows silently, so a guard there would turn those into
raises. At 0.3.535
the 60,000-80,000 ones grouped as 90 `cannot be related` (the WHERE-gate
host below), 54 wrong answers, 44 keyless joins, 15
recursions, 8 `Could not resolve connections`, 8 `Missing source`, 8
`Ambiguous reference` (below).

- **A set planned under the WHERE reads unfiltered rows** when its condition
  reads an aggregate the WHERE is hosted on: `select city ? city in (city ?
  max(height) by * > 0 and height = 5) as f where min(height) by species < 4`
  returns A (the answer is empty). 0.3.534's re-application of such a gate
  (`_unfiltered_filter_streams`) is switched off inside `_CleanFeederCache`
  and a HAVING membership's subselect (`planning_a_set`): there it cost about
  2,000 joins over 40,000 queries and broke six (a duplicate CTE alias, a
  keyless join, wrong rows in 49404). Reading the set's rows off the
  statement's WHERE-kept stream, which the efficiency lead in area 4 asks for
  anyway, would fix both.
- **A filter's claimed grain is still the select's.** 0.3.535 has a shown
  filter only FINAL reads ask for the shown keys of it (`_group_needed`),
  not a set's filter, one another group reads (77134) or a filtered
  aggregate keyed by shown keys (73232): each broke seeds right on main. It
  costs 1,428 joins on queries main answered; giving the filter group its
  real grain at graph construction would retire the rule and the claim.
- **The set's inner aggregate supplies a select key** (9919, 28312: raised
  on main, wrong rows now): in `select species, (min(height) by tree_form) ?
  tree_form in (tree_form ? min(height) by city, species >= 0) as f` the
  `by city, species` aggregate feeds the set and the `by tree_form` roll-up,
  so it is no set-only input (0.3.536 excludes those from the cover). FINAL
  joins it on `species` and does not group, so Acer shows once per city.
- **A derived key beside a filtered column off a peel joined on nothing**
  (83978): `select city, case when tree_form = 'broadleaf' then 'b' else 'o'
  end as fc, (min(height) by city) ? city in (<set>) as v0 where tag in
  (<set>)` takes `fc` off the species peel and `city` off the filter's rows,
  and FINAL joins them `on 1=1`. With `tree_form` in place of `fc` it is
  right. `_keys_off_one_tuple` reads only KEY-purpose columns. Reduced,
  `select city, tree_form, (min(height) by city) ? city in (city ?
  max(height) by tree_form is null) as v0 where tag in (<set>)` reads
  `tree_form` off the set's `by tree_form` aggregate beside the filter's
  `city` (`Filters.set_input_supplies_no_output`; 17550 too). Excluding
  every set-only input from the cover fixed it and broke four seeds right
  on main (9515: a set's `by city, species` aggregate is the right source of
  the WHERE-kept pairs).
- The gate backfill skips a total an atom reads beside a membership
  (`total > x or tag in <set>`): backfilling the OR raised 182674, 185850 and
  186115 with `Could not resolve connections`. 0.3.543 removed a cause of that
  error (`all_rows_marker_is_no_key`), so retry the OR backfill.
- **A set over nullable content keeps the filter's padding NULLs.** The set
  of `x ? c` is the values of the rows `c` passes, a NULL included where a
  passing row's `x` is NULL; matched by identity (`_null_free_set`). The set
  is rendered as the CASE stream, so where `x` is not null-free the rows `c`
  rejects add NULL members: `select tree_id where city = 'A' and max(height)
  by species in (max(height) by species ? species = 'Acer')` keeps the
  Ulmus `a4` (the set is `{7}`; on main too). Build such a set off the rows
  `c` keeps. The generators write only `col in (col ? c)`, so no family
  reaches it.

- **Decide: a condition its keys do not decide.** For a filtered property
  beside its key or its content the reading is pinned
  (`tests/engine/test_filter_concept_is_a_value.py`: `select name, bulk_name`
  is the name if any of the product's orders passes, one row per product).
  A filtered key beside an unrelated key is not: `select city, species ? tag
  = 'x' as f` collapses per `(city, species)` (no `(A, NULL)`; `(B, NULL)` for
  B's Acer), and adding `count(tree_id)` returns the per-row reading (`(A,
  NULL, 1)`), so a count changes the keys' rows. The generator stays off
  these (`Filters.filtered_value_off_its_keys_splits`).
- **9683, 12294 raised on main and return wrong rows**: a WHERE aggregate
  reaches the set's inner aggregate and not the statement's rows (below).
- **Where a WHERE aggregate is hosted beside a filtered column** is the lead
  with the most behind it (9683, 12294, 2922, and 92 `cannot be related`
  raises). 0.3.531 backfills the gate onto the outputs when its only
  consumer is an aggregate inside a set (not an output, and only under a
  WHERE that reads no set itself), and stops counting a set the host reads by
  subselect as a row barrier. Three wider rules each fixed seeds and broke
  more, and are not in: hosting the gate on any shown filtered column (14
  windows seeds, 40 here: the column's group cannot render every gate);
  reading a barrier that feeds only a filter's condition as unhosted; and
  preferring a statement-row host over a side-channel host that carries the
  gate's grain (17300, 18372). 0.3.537 gates a filter only FINAL reads
  whatever its grain (2922), and hosts a WHERE on the statement's rows rather
  than on a group only sets read (117080 and 29 more); that raises on five
  seeds (8532, 17300, 18372, 39289, 50368) where the statement's remaining
  hosts cannot carry the gate's grain. Keeping the set host when its members
  decide the gate's grain fixed three of them and made six wrong again, so it
  is not in. 0.3.536 also backfills under a WHERE that reads a set, unless
  the gate is a total (above); with the rest of 0.3.536 that fixed 75 of 139 failing seeds in
  80,000-100,000.
- **Left after 0.3.537** (seeds 100,000-120,000, each failing on main too):
  a set's row stream beside a gated total reads unfiltered rows (115173;
  hosting the gate on filter groups under `IN_SET_PLAN` raised `Missing
  source reference to height`); on the summary model FINAL reads root rows
  without grouping once `_keys_off_one_tuple` moves `city` and `species` to
  the root (107276); a nested set plan reaches an outer FilterNode holding
  `v0`/`form0` and recurses (111857); a WHERE `tag in <set> or ...` (117965);
  windows over filtered keys (104811, 111761, 119253); and 14 keyless-join
  raises (100817, 102885, 105862, ...). 0.3.537's rule that a total decides
  no key (`total_splits_no_key`) turns 91381 into a keyless join, and the
  content-hosted gate with the filtered-key rule (113128) turns 59678 and
  72714 into keyless joins: each beside a window or a derived key.
- **A derived key regrafted onto a group its FINAL peer pairs with on a
  coarser key** (112517, base): `select fc, tag ? city in (<set>) as v0,
  min(height) by * as v1` reads `fc` off the `(city, tree_form)` minimum the
  total rolls up, and FINAL meets the tags on `city` alone
  (`Filters.regraft_keeps_paired_key`). Counting FINAL as a pairing reader in
  `_regraft_candidate` fixed it and broke TPC-DS q25 (a WHERE placed past the
  aggregates) and q66 (+20% SQL); a peer that shares or determines the
  dropped key did not separate them either.
- A filtered total beside rows is one value for the statement: `select
  species, tag, (min(height) by *) ? tag = 'x' as f` shows 2 on the y row
  (the constant twin `1 ? tag = 'x'` is per row). Giving it the condition's
  keys at parse time (`filter_item_to_concept`) is the fix, but FINAL then
  pairs the mask with the rows on nothing: it needs the row stream as bridge.
  The generator writes no `by *` content beside a `?`.
- Keyless joins raised: a filtered key beside a bare aggregate and a `by
  tree_form` value (5172). 0.3.540 widens a projection that dropped a root
  key it read only for its WHERE (2486 and four more `OPEN` seeds); the rest
  are 169058, 170854, 172607, 177741, 178181 and 178274 (summary).
- **Left after 0.3.540** (seeds 160,000-180,000, each failing on main too: 5
  wrong, 43 raises, 6 invalid of 40,000): windows over filtered keys
  (162928, 174664); the derived key off a peel (175744); nested-aggregate
  statements DuckDB rejects (162190, 164009, 172828, 178097); recursion
  (161496, 171031, 177584; fixed in 0.3.544); `cannot be related` and `Could not resolve
  connections` (163611, 165374, 172055, 173363, 173737). 18629 needs a
  filtered aggregate's step deduped as a lookup: 0.3.540 dedups a filtered
  step only when its filtered column is a pairing key
  (`lookup_side_counts_once`), since deduping one whose content is an
  aggregate (175585) or a row identity (96573, 28859) broke seeds right on
  main. Seeds 117,000-160,000 were not compared against main this round.
- **Left after 0.3.541** (seeds 180,000-200,000, each failing on main too: 4
  wrong, 32 raises, 7 invalid of 40,000):
  - 198442: a window `over (partition by v0)` is planned as an aggregate by
    `v0` and FINAL reads `(city, tree_form, v0)` off the filter's rows, which
    never met the gate `sum(height) by species = 0` (its host is the
    count by `city, tree_form`, a side FINAL does not join). `elect_cover`
    ranks by `rows_filtered`, but `_where_atoms` holds only atoms that hold
    on an absent group, so for this gate every candidate passes and the
    most-downstream filter wins. Reduced: `select city, tree_form, city ? not
    (city in (city ? count_distinct(tag) by city, species < 1)) as v0,
    sum(count(tree_id) by city, tree_form, v0) over (partition by v0) as v1
    where sum(height) by species = 0` (the answer is empty). Ranking by every
    WHERE atom is the candidate fix; it touches every cover, so sweep it alone.
  - Raises: keyless joins (184369, 184759, 185251, 194927, 199620), `Could not
    resolve connections` (184974, 185997, 187204, 190792, 195260, 198604),
    recursion (191855, 197547, 199349; fixed in 0.3.544), `renders no column` (183765),
    `Missing source reference` (193049). Invalid on DuckDB: a CTE alias read
    for a column it lacks (197959), nested aggregates on the summary (183562,
    198529, 199618), a missing `_virt_agg_sum` column (192798).
- **Left after 0.3.542** (seeds 200,000-220,000, each failing on main too:
  wrong 198442 above, 214126, 216029, 217166, 217347; 14 raises):
  - Totals at keys: widening a WHERE value to its atom's grain
    (`gate_grain_is_its_atoms`) also fixed 12294, 25230, 78425 and 91969
    (`min(height) by city, tree_form - max(height) by * < 0`), but widening
    a `by *` total broke 48509, 51668 and five more right on main, with the
    same atom shape: what tells them apart is in the select, not the WHERE.
    An atom reading a set is not widened either (4617).
  - 204389: `select height > 2 as big, tree_form ? min(height) by tree_form
    != 1 as v0, (sum(height) by big) ? sum(height) by * >= 0 as v1` pairs
    the two filters on nothing (nine rows for four). Each filter is built
    over its aggregate's groups, yet is taken to hold the other's column and
    `tree_id`. Dropping those from its identity fixed it and broke 99325,
    118586, 131528, 167037 and 188314: there the filter's row parent carries
    a WHERE gate's key FINAL needs.
  - 208906 lost its summary read on the branch (no extra scans): the peel a
    set now skips (`peel_skips_a_set`) was what let the summary cover it.
    So did 7011, 19935, 51558, 53696, 61983 and 161167 over 0-220,000, against
    402 gained.
  - Raises: 201703, 204919, 207061, 207567, 208889, 209669, 211780, 212468,
    213174, 213328, 213931, 216560, 217204; 208977 is invalid on the summary.
- **Left after 0.3.544** (seeds 240,000-260,000 and older; each fails on
  main too):
  - Sweep the whole range before trusting a slice. Taking every set reader
    out of a set's grain (`sets_key_no_readers`) fixed the recursions and
    broke nothing in 240,000-244,000, but over 0-260,000 it broke 202 seeds
    (a set in the WHERE beside a filtered select column, which keeps that
    column as a key: 3343); a window gated like an aggregate broke 187656
    (`row_number() over tree_form` beside aggregates by `tree_form` numbers
    groups). Both are narrowed: other readers leave a set's grain only beside
    its own reader, and a window at the grain of the select's aggregates is
    no host.
  - 217347: `select city, species, tree_form, tree_form ? species in (species
    ? min(height) by tree_form > 8) as v0, avg(min(height)) over (partition
    by v0) as v1 where count_distinct(tag) by city, tree_form >= 2` returns
    every tree. The `v0` filter reads the raw rows, and the set it reads is
    planned standalone (its aggregate reads the WHERE-kept rows), so in the
    statement's group graph nothing links `v0` to the gated host: neither
    `_unfiltered_filter_streams` nor a FINAL restatement (FINAL holds no
    value of the gate) can reach it. With `height > 8` in place of the
    set's aggregate it is right. Build the filter off the gated rows.
  - A filter whose condition reads an aggregate is no bridge FINAL widens:
    `select species, city ? avg(height) by city > 1 as v0, sum(height) by
    tree_form as v1, count(tree_id) as v2` raises a keyless join (248230;
    with `city = 'A'` it plans). The filter's node outputs only `city, v0`
    though it carries `species`, so `_root_lookups` finds no `tree_form` for
    it. Adding its carries there elects it as the bridge, but FINAL passes
    only the cover's two aggregates through `_carry_planned_keys`, so the
    filter is never widened and the join stays keyless.
  - A derived key needs a pair bridge: `select height > 2 as big, (sum(height)
    by big) ? min(height) by * != 5 as v0, tree_form ? count(tree_id) by * >
    8 as v1` raises a keyless join (245208): the `by big` filter and the
    species-grained one share no column, and only the row stream and the
    `big` scan together hold both keys. With `city` in place of `big` it
    plans.
  - Raises 255107 (keyless join), 259271 (`renders no column`), 225806 and
    237588 (keyless joins); invalid 240270 and 228836 (a CTE alias read for a
    column it lacks), 240373, 237225 (nested aggregates on the summary),
    243768 and 250022 (a column outside the GROUP BY on the summary).
  - A derived key off a peel (`_peeled_key_inputs`) pairs a filter on its
    inputs' keys. 83978, 141488, 146267 and 175744, the older peel leads, are
    still wrong: their key comes off a peel the WHERE gates.
- Hash order: seed 14276 planned right or wrong by `PYTHONHASHSEED`, on main
  too (predicate pushdown read a column off a set feeder outside the FROM;
  fixed). It still renders two SQL texts by hash seed, both right: some
  choice iterates a set. Run a law suite under two hash seeds before listing
  a seed as a strict xfail.
- A plan that loops: seed 2917 took more than 30 s on main (a window over
  two filtered keys under a WHERE gate); the sweep now records a timeout.
- Efficiency: 0.3.536 reads 81348 and 99524 with two scans where main read
  one: a set's grouped `HAVING avg(height) by species is not null` scan no
  longer merges with its row twin under the same WHERE.
- Efficiency: a filtered key beside a derived key dedups through a second
  contributor joined back on the key's input (`select coalesce(tree_form,
  'none') as form0, tree_id ? species = 'Acer' as f` self-joins on
  `species`): the filter's row stream renders `form0` but its group does not
  output it, so the cover cannot take it from there. 0.3.532's dedup of a
  filter stream to its grain costs 45 joins over 56 queries of 20,000-40,000.
- The family writes no filtered column inside a window's own argument, no
  `?` over a derived key, and one condition per select when the filters are
  alone (two conditions alone, `select tree_id ? c1, species ? c2`, keep the
  rows both reject).

### 5. ClickHouse (`Padding.lean`)

- A product of decimals overflows at 18 digits whatever the declared type
  (loud); cast to the declared type first.
- `written_nullable` reads every CTE's nullable set, so a filtered column is
  still declared `?`. A persist into a declared datasource is not checked for
  NULL at write time. `x / (4 / 2)` is nullable (fold constants first).
- `round` of a tie not exact in binary is held to no decimal reference.
- `lag`/`lead` of an array operand would fail `toNullable`.

### 6-7. String matching and functions (`StringMatch.lean`, `Strings.lean`)

- Collation: MySQL and SQL Server fold case in `like`, `=`, `LOCATE` and
  `CHARINDEX`. Unicode case differs per engine; the suites are ASCII.
- A computed pattern ending in a lone backslash, `char(1..3)` on SQLite, `[`
  on SQL Server. A Trilogy string literal cannot end in a backslash.
- A prefix pattern (`'Bos%'`) implies a range a partition could read.
- SQLite has no `regexp_*` or `split`. Postgres, MySQL, SQL Server and
  Snowflake render `REGEXP_*` functions they lack; BigQuery's extract drops
  the group.
- The clamped substring names a computed start up to 8 times (19 on dialects
  without an engine in CI).

### 8-11. Casts, readings, instants, writings (`Casts.lean`, `Readings.lean`, `Instants.lean`, `Writings.lean`)

- **Dialects with no engine in CI** (Postgres, BigQuery, Presto, Snowflake,
  SQL Server) render a bare CAST for string reads and writes: they raise
  where the reference is NULL or rounds. Each needs its regex function.
  MySQL is pinned, not run, and leaves a zone unread.
- **The width of `int`**: 32-bit on DuckDB, 64 elsewhere. One width would
  change every DuckDB DDL.
- **Precision**: microseconds on DuckDB, milliseconds on ClickHouse, seconds
  on SQLite.
- **A date with a zone** is NULL; either reading is a decision.
- Overflow of integer arithmetic and float-to-int casts differs per engine.
- Float ties not exact in binary, and long fractions, round by the binary
  value on SQLite. SQLite writes a 16-17 digit double with a last digit that
  is not `repr`'s. A decimal past 15 digits is approximate there.
- `round(n, 2)` and `avg(n)` of a `numeric(12,4)` are declared
  `numeric(12,4)`.
- NaN, year edges, ClickHouse dates before 1970 and its server zone, ranges
  past Decimal128, Unicode digits: the suites stay inside the common range.
- SQLite lacks `stddev_samp`, `var_samp`, `any_value`, `array_agg`, hashes
  and `date_diff(..., quarter)`. ClickHouse's `groupArray` of an all-NULL
  group is `[]`.
- SQL size: each cast carries 1.5-5 kB of text; binding a typed column to a
  `string` concept pays the read guard for nothing.

### 12. Sharing (`Sharing.lean`)

- `random` (area 3) is the wrong-answer lead; a column-free volatile operand
  on SQLite resolves with it.
- CI does not run `test_sqlite_reads_parse_before_3_46` (its SQLite is too
  new). Three nested instant reads still overflow SQLite 3.45's parser.
- DuckDB names an instant read's operand four times, so nested reads grow
  4^n in text.
