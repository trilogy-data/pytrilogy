# Handoff: optimizer phase-ordering audit (2026-10-09)

## Resolution: the plan runs to a fixpoint

`optimize_ctes` repeats the rule plan until a pass changes nothing
(`MAX_OPTIMIZATION_PASSES`, warns if hit). Pass 1 is the old single pass. On a
later pass a phase reruns only if some phase changed the plan since it last
ran (each phase already loops to its own fixpoint), and `refires_after`
gating applies on pass 1 only: a later pass sweeps whatever any phase left.
The per-phase body is `_run_phase`; the plan list itself is unchanged.

18 corpus plans move (q59 by the pushdown fix below), all same rows (repro
differs only under its unordered `LIMIT`), exec time neutral to slightly
faster: 9 fewer CTEs, +0.1% chars.
q31 (+1.6k chars) and adhoc03 grow because a ratio folded into its GROUP CTE
re-renders the aggregates it reads (no same-SELECT alias); q04 now plans as
one aggregate with its comparison in HAVING. adhoc03 renders its ratio over
COUNT(DISTINCT) correctly (see `handoff_count_distinct_colocation.md`).

The fixpoint exposed `PredicatePushdown` moving one WHERE atom per run (q04
took 6 passes): after the first atom landed on a parent, the parent's
condition was tested for scalarity without the parent's materialized
columns, so it read as non-scalar and blocked the rest. It is parent-relative
now, like the candidate check; q04 settles on pass 2.

Guarded by `tests/optimization/test_optimizer_fixpoint.py`. The
Join/BaseJoin item below is resolved too.

Why: the existence fold (`optimizations/existence_having_fold.py`) needed three
re-fires wired by hand (`predicate_pushdown`, `union_dim_pushdown`,
`inline_datasource` `.after_existence_fold`), each found only after a plan came
out worse. The optimizer is one ordered pass over the rule plan: a rule runs
again only when a later phase names it in `refires_after`. So the question was
how many other rules leave work behind for an earlier one.

## Method

Patch `build_optimization_rule_plan` to append a second copy of every phase
(names `#pass2`, no `refires_after`, so it always runs) and diff the SQL
against the normal single pass. Scripts used (not committed):

- corpus: `local_scripts/sql_ab/corpus_sql.py` under the patch, then
  `sqldiff.py once.json twice.json` (202 statements, 15s).
- attribution: wrap each pass-2 rule's `optimize` and record which phases
  report a change per moved statement.
- suite: `-p sqlcap -p <fixpoint plugin>` over the whole suite.

## Findings

**One real bug, fixed in a5295ad8a.** Re-running `inline_datasource` on
TPC-DS q84 rendered `INVALID_ALIAS`: the filtered-parent fold moved a scan's
WHERE over a presence probe the scan computes onto a consumer that binds only
raw columns. A single pass never reached it, but the existence fold's
`inline_datasource` re-fire can. `_can_inline_filtered_parent` now requires the
WHERE to read raw columns only; guarded by
`tests/optimization/test_inline_datasource_rerun.py`.

**18 of 202 corpus statements still move on a second pass.** Every one returns
the same rows (sf=1; `repro.preql` differs only under its unordered `LIMIT`).
None is wrong rows; they are plans a fixpoint would finish:

| phase that still changes things | statements | effect |
|---|---|---|
| `predicate_pushdown.remove.after_join_upgrades` | 9 (q04 q31 q44 q74 q78 q80 repro, hn adhoc06/07) | drops a WHERE atom a parent already applies; -15 to -160 chars |
| `hide_unused_concepts` | 8 (q16 q23 q64 q74 q94 q95, hn adhoc06/07) | prunes columns later rules stopped reading |
| `collapse_single_parent` | 6 (q16 q31 q64 q94 q95, ncaa adhoc03) | -1 CTE on q16/q64/q94/q95; **q31 grows** 3,819 -> 5,633 chars (rows and time same) |
| `predicate_pushdown.remove` / `.initial` | q04 q11 q23 q74 | shrinks the fold's queries further (q04 -770, q11 -160 chars) |
| `union_dim_pushdown` | q23 | |
| `upgrade_join_on_guards` (both) | q73 | |

The suite run under two passes: 8 failures, none wrong rows and no
`INVALID_ALIAS` anywhere. 6 are pipeline-shape tests (they assert the phase
list), `test_thirty_one` is the q31 size budget above, and
`test_aggregate_condition_feeder_keeps_only_value_and_grain_contract` counts
`'EUROPE'` occurrences (one redundant copy goes away).

### Options

1. **Add the cheap re-fires.** A late `hide_unused_concepts` and
   `predicate_pushdown.remove` re-fire (both shrink-only) and a
   `collapse_single_parent` re-fire after the late rules would take most of
   the table. q31's growth under collapse is understood and acceptable: the
   folded ratios re-render the hidden aggregates (no same-SELECT alias), but
   DuckDB computes each once and the run time is unchanged. The same
   re-derivation exposed a real bug (`ncaa/adhoc03`: a COUNT the planner
   rendered DISTINCT re-derived as a plain COUNT), guarded in 3adac04e6 and
   written up in `docs/handoff_count_distinct_colocation.md`.
2. **Run the plan to a fixpoint** (repeat until no phase changes anything).
   Simplest and most robust, roughly 2x optimizer time on the statements that
   move, and the pipeline tests would assert phase order differently.
3. Keep the audit as a CI check instead of a fix: the second-pass corpus diff
   takes 30s and names the phase.

## Join vs BaseJoin (resolved 2026-10-10)

Each CTE used to carry its joins twice, `cte.joins` (`Join`, rendered) and
`cte.source.joins` (`BaseJoin`, from the QueryDatasource), and six rules read
or wrote the BaseJoin copy. Join-level upgrades never reached it, so 52 of 614
corpus BaseJoins were wider than the join that rendered.

Now no optimizer rule reads or writes `cte.source.joins`; `BaseJoin` is the
plan-time join over datasources and `Join` is the only join after CTE build.

- `join_upgrade`: the early pass (`upgrade_join_on_guards.early`, was
  `.base_join_only`) narrows LEFT `Join`s to INNER on the CTE's own WHERE.
  The datasource-level proof path (`_downgrade_base_join`, datasource keys)
  is gone. Its 12 corpus firings were all on joins the later pass narrows
  anyway: syncing the Join in that pass moved 0 plans.
- `union_dim_pushdown`, `join_hoist`: match dims through
  `join.right_cte.source` and build only the `Join`.
- `merge_irrelevant_group_by`: `join_preserves_left_rows` takes a `Join`.
- `predicate_pushdown`, `reuse_parent_lookup`, the existence fold: dropped
  their BaseJoin reads and writes.

One plan moved: q83 (and `_q83_with_sales_measure`). Its date dim joined
FULL in the stale BaseJoin while the Join rendered INNER, which blocked union
dim pushdown. The dim and its week filter now sit inside each union branch:
same rows, 120ms -> 72ms at sf=1, +350 chars.

Then unified into one class: `Join[Node]` carries a join at every tier.
`NodeJoin` (strategy nodes), `BaseJoin` (datasources) and the CTE `Join` were
three copies of one shape; `ConceptPair`/`CTEConceptPair`,
`GuardTerm`/`CTEGuardTerm` and `UnnestJoin`/`InstantiatedUnnestJoin` the
same. `bind(node_for)` carries a join to the next tier's nodes
(`MergeNode.translate_node_joins` resolves, `base_join_to_join` maps
datasources to CTEs) and `repoint(node_for)` rewrites one in place
(`replace_dependency`, union dim pushdown, graph canonicalization). The
aliases `SourceJoin` / `CTEJoin` / `NodeJoin` name the tiers in annotations.
0 corpus plans move.

A concept-keyed join (`concepts=`, no pairs) keeps its concepts while
planning: planning reads it apart from pairs
(`grain_utility._joins_a_coalescing_relation` treats it as a member with no
pairing to read), so folding it into pairs early would not be a refactor.
`bound_keys()` folds it when the join is bound onto CTEs, as before.

`QueryDatasource.joins` is now plan-time only: nothing reads it after CTE
build. Its last reader, `strip_redundant_not_null`, walked the whole QDS tree
to the leaf tables to get "could be NULL before this CTE's WHERE". That walk
discarded every parent's real filters and trusted the stale join copy. The
rule now reads one level: the parents' `nullable_concepts` (narrowed only by
filters that ran), its own tables (an inlined parent's raw table, since its
WHERE may have folded into this CTE) and its own outer joins' padding. q64
drops one more guard (`C_CURRENT_ADDR_SK is not null` beside an INNER
customer join, declared non-null; same rows).

### The rest of the QDS is NOT a mirror

The rules also write `cte.source.datasources` / `source_map` /
`input_concepts`. Deleting those writes in `join_hoist` breaks q35 and q69
with `INVALID_ALIAS`: after build, the QueryDatasource is the CTE's FROM
model. `base_datasource` picks the FROM table (`source_address`,
`base_alias`, `quote_address`), `CTE.get_alias` falls back to
`self.source.get_alias` for a raw or inlined table's columns, and
`inline_parent_datasource` rewrites it deliberately so later rules see a
direct scan. Removing it means giving the CTE its own FROM model
(base table + inlined tables + parents as bindings) and moving the render
lookup onto it: a redesign of the render path, not a cleanup.
