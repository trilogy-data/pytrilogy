# Handoff: COUNT(DISTINCT) colocation renders one aggregate two ways (2026-10-09)

## Resolution (44012044d)

- **Kept the colocation.** It produces TPC-DS's canonical
  `count(distinct order_number)` single scan; the split plan is 4 -> 8 CTEs
  on q16/q94/q95 and 3 -> 8 on q83. It fires on 6 corpus statements.
- **No concept copy.** DISTINCT is a render choice of the stream, recorded
  as addresses: `StrategyNode.distinct_counts` (a mark, kept by copies) ->
  `QueryDatasource.distinct_counts` -> `CTE.distinct_counts` (merged like
  `zero_filled`, carried by `carry_child_state`). The dialect renders COUNT
  as COUNT(DISTINCT) for those addresses in that CTE, whoever renders them.
  A folded ratio and q83's HAVING now render the count the way its column
  does. `_apply_count_distinct_rewrites` is gone.
- **Soundness hole found and closed.** Folding onto a sibling whose inputs
  are bound by a datasource marking the counted key `~` counted only that
  fact's subset (`count(order_id)` beside `sum(cost)` over a `~order_id`
  shipments fact: 1, not 3). `ConceptAttrs.aggregate_partial_keys` blocks
  that target. A partition's `complete where` partiality (healed by the
  covering union: ncaa `game_tall`, TPC-DS unified sales) still folds.
  Filtered counts and NULL keys are sound (DISTINCT over a CASE drops the
  same rows COUNT does).
- **Remaining copies closed.** The first-row read (`_read_first_rows`),
  region-named arguments (`_name_inline_arguments`) and
  `filtered_aggregate._remove_filter` still rewrite a copy under the
  aggregate's address, but the copy is the CTE's own output column, and the
  renderer re-derives an aggregate the CTE computes from that column
  (`dialect/base.py::_own_aggregate_definition`), never from the lineage a
  consumer holds. A scalar folded into the pass reads the first-row marker
  and the named argument the pass reads, and since a CTE's reads are what it
  renders (`render_cte_used_map`), `hide_unused_concepts` keeps those
  columns. Collapse's `_reads_aggregate_differently` guard is gone.
  Before the change, with the guard removed and collapse run twice,
  `a * 2` over a first-row `sum(amount)` gave 120 for 60 and a named
  argument failed to bind:
  `tests/engine/test_derived_key_domain.py::test_scalar_folded_into_pass_reads_its_rewritten_aggregate`.

## The problem

When the planner computes a count of a key in the same aggregate pass as
siblings that read a finer row stream, it renders that count as
`COUNT(DISTINCT ...)`. It does so by rewriting a **copy** of the concept on the
aggregate node's outputs. Every other concept still references the original,
whose lineage is a plain `COUNT`. Two concepts now share one address with
different lineages, and anything that re-derives the aggregate from lineage
instead of reading the column gets the non-DISTINCT one.

Observed in `tests/modeling/ncaa/adhoc03.preql`:

```
count(game_tall.id ? game_tall.is_home = true) as home_games,
home_wins / home_games as home_win_rate,
```

The aggregate CTE renders `count(distinct CASE WHEN is_home THEN id END) as
home_games`. `home_win_rate`'s lineage still points at the original
`home_games` (plain COUNT). As long as the ratio lives in a downstream CTE it
reads the `home_games` column and is right. When `collapse_single_parent`
folded the ratio into the GROUP CTE (only reachable by running the optimizer
plan twice), SQL cannot reference a select alias in the same SELECT, so the
renderer re-derived the count from the ratio's lineage:

```
count(distinct CASE WHEN ... THEN id END) as home_games,
sum(...) / count(CASE WHEN ... THEN id END) as home_win_rate
```

Rows matched on the fixture data only because each team's game id appears
once in that stream. On data where the stream repeats ids it is silently
wrong.

## Where it lives

- Decision: `v4_helper/group_rules.py`.
  - `_fold_distinct_rewritable_buckets` (~line 224) folds a bucket whose
    members are all distinct-rewritable COUNTs onto a sibling bucket with a
    finer input grain, recording `aggregate_distinct_addrs`.
  - The bucket assembly at ~line 496 sets `aggregate_distinct_addrs` for
    counted keys whose own input grain is not the counted key, or when a
    sibling's input leaves a residual grain.
- Rewrite: `v4_helper/strategy_builder.py`, `_apply_count_distinct_rewrites`
  (~line 5633), called at ~line 5811 on the node's `outputs` only
  (`dc_replace` on the concept and its lineage; same address).
- Sibling mechanism with the same shape: `aggregate_first_row_grains` /
  `_first_row_grains` (group_rules.py) and `_read_first_rows`
  (strategy_builder.py) handle non-count aggregates over a repeated stream by
  reading the first row per tuple. Check whether it has the same
  same-address/different-lineage exposure.

## What is in place now

(Historical: the guard below was removed once the renderer read a CTE's own
aggregate definition; see Resolution.)

`3adac04e6` added a guard, not a fix: `collapse_single_parent`
(`basic_fold_into_group_is_safe` -> `_reads_aggregate_differently`) refuses to
fold a scalar into a GROUP CTE when it reads a parent aggregate through a
lineage different from the one the parent renders. Test:
`tests/optimization/test_collapse_basic_into_group.py::test_fold_never_renders_a_rewritten_count_two_ways`
(re-runs `collapse_single_parent` at the end of the plan; fails without the
guard). No single-pass corpus plan moved. Any other path that renders an
aggregate from a consumer's lineage instead of the column is still exposed.

## Direction from the owner

- Do not mutate the concept in place: **swap in a new concept** (its own
  address, COUNT_DISTINCT lineage) so no two concepts share an address with
  different lineages. Open question: how dependents (`home_win_rate`) come to
  reference it, since their lineage holds the original object. The decision
  is made at grouping, so the swap probably belongs there or earlier, not at
  node build.
- The owner is **suspicious of the colocation itself**. Before engineering
  the swap, establish whether folding a key count onto a finer sibling stream
  is sound and worth it:
  - Is `count(k)` over the sibling's stream, made DISTINCT, always the count
    over `k`'s own population? Filtered counts (`count(id ? cond)`), NULL
    keys, and a stream that drops rows (INNER joins below the shared pass)
    are the cases to probe. The docstring of
    `_fold_distinct_rewritable_buckets` already rejects FD closure for one
    such reason.
  - What does it buy? Compare against the split plan (own pass + re-join at
    the output grain) on the TPC-DS / thelook queries that hit it. If the
    win is small, removing the colocation removes the rewrite and the bug.
  - `COUNT(DISTINCT)` is usually slower than `COUNT` over a deduped stream on
    large inputs; the CTE it saves may not pay for it.

## Starting points

- Find the queries that use it: instrument `_apply_count_distinct_rewrites`
  (or log `aggregate_distinct_addrs`) over the corpus
  (`local_scripts/sql_ab/corpus_sql.py`) and the suite (`sqlcap`).
- Wrong-rows probe: a model where the shared stream repeats the counted key
  (a fact joined to a finer child), a ratio over the count, and the optimizer
  plan run twice (see the test above for how to re-run one phase). Assert
  rows against hand-written SQL.
- The optimizer audit that surfaced this is written up in
  `docs/handoff_optimizer_phase_audit.md`.
