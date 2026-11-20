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

**State at 0.3.524.** Areas 0-12 below each have a module and a law suite. The
`first` and `spellings` oracle families were swept to seed 450,000, the
`expressions` family to 800,000 and the `compositions` family (memberships,
`/` by 0, derived `by` keys, nested aggregates, ranks, `order by` an
expression) to 90,000; the PR of each round holds its sweep totals. Only
`compositions` has `OPEN` seeds (20 queries, area 1).

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
  and cannot see a bug that needs an earlier statement's definition.
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
  narrowing included, and the broad suite after the last one. Re-sweep the
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
  column is carried across its existence edge (0.3.524 fixed one of each).
- `existence_arguments` of a literal `in (...)` is `[()]`, truthy: test with
  `any(...)`. Three sites in `condition_placement.py` still test truthiness.
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
  wrong or raising seed on main. Sweep `compositions` instead; it is far from
  spent (1 query in 125 was wrong on main to 30,000).
- Efficiency: `MergeTwinScans` reads a scan under a WHERE off an unfiltered
  twin of its table. Left (about 3,300 of 300,000 queries still scan twice):
  a filtered row scan beside a grouped, unfiltered one with no row twin
  (`select tree_id where city = 'A' and count(tree_id) by species > 1`), and
  a summary re-sum beside its row twin (it reads stored partials through
  `partial_sources`, which the rebase would have to carry).

Open in the `compositions` family (`OPEN` in
`tests/discovery/test_oracle_compositions_laws.py`, each failing on main):

- Summary only (a membership or a `by *` gate beside a derived key or a
  nested aggregate): 15155, 23531, 34415, 37601, 38002, 47456, 71000, 77768,
  83051, 84985, 85861, 89278. Two are reduced:
  - `select city, count(tree_id) where count(tree_id) by species != 3 or
    city in (city ? tag = 'q')` reads the per-species count off the stored
    `(city, species)` count, re-sums it to city and applies the atom as a
    city HAVING (37601's shape). Without the membership on the select's own
    key it plans right; the plan goes through `_finer_filter_reaches_rollup`.
  - A set's `max(height) by *` beside the same total in the WHERE is re-maxed
    per species off the summary, so the set compares a per-species max as
    the total (47456).
- `Graph contains a cycle` (41277, 66218, both models): `max(height) by city,
  tree_form - sum(height) by tree_form = 2 or (tree_form in (tree_form ?
  count(tree_id) by tree_form != 10) and tree_form in (tree_form ?
  max(height) by city, tree_form is null))`. The OR reads both sets and is
  hosted on the aggregate groups that compute the sets' own inputs (one
  concept is both a set's inner value and the gate's argument), so
  `_split_cyclic_groups` cannot split it. Host the atom below neither set.
- Raises (summary): `Merge sources` (5754, 32125), `keyless join` (89278),
  DuckDB's `Ambiguous reference` (32239, 71000) and `Missing source reference` (13009: a rank beside `max(height)`
  under `avg(height) by * is not null`). 13009, 5754 and 32239 plan when
  `_split_root_dimension_clusters` peels no aggregate onto a single-key
  dimension, but that peel is how the summary serves `max(height)` beside
  `city` (`test_rollup_resolution_laws.py` `max_beside_avg`), so the guard
  was dropped: find what 13009's peeled `species` bucket lacks instead.
- Not generated yet: windows other than `rank` (`lag`, `row_number`, frames),
  a membership in a select target's `?`, a membership in a HAVING, `/` by a
  `by` aggregate that can be 0.

Reads the summary could serve:

- `min(count(tree_id) by tree_form) where city = 'B' and species = 'Acer'`
  (first 446174) now reads the trees: the summary answers it with the WHERE
  on its scan, below the re-sum.
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
  side; fixed where it bit (0.3.500), not in general.
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
