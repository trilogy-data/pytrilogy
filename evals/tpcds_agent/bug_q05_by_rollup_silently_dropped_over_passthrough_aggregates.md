# q05 token sink — `by rollup (…)` SILENTLY DROPPED when the SELECT has no fresh aggregate

**Target:** q05 in `evals/tpcds_agent/results/20260703-212501/` — **1,640,409 tokens, FAILED**
(100 cand rows vs 100 ref rows, "result set differs from reference"). Trilogy v0.3.288.

## Symptom (what the agent saw)

q05 = channel roll-up (store/catalog/web) with `rollup(channel, entity_id)` →
per-entity leaves + per-channel subtotals + a grand total, sorted nulls-first.

The agent authored the natural Trilogy shape: two `where … select …` rowsets
(`sale_agg`, `return_agg`), each pre-aggregating with `sum(…)`, combined with a
`union join`, then a final SELECT that PASSES THROUGH the pre-aggregated columns
(`coalesce(sale_agg.ext_sales, 0) as total_ext_sales`) with a
`by rollup (sale_agg.channel, sale_agg.entity_text_id)` clause.

The submitted query runs clean and returns 100 rows — but **every row is a leaf
(catalog channel); there are ZERO subtotal/grand-total rows.** The `by rollup`
clause was silently discarded. The agent noticed and could not explain it,
churning for hundreds of thousands of tokens:

> "all 442 rows have non-null channel_label and entity_id. That means the rollup
> subtotals aren't showing?" (conv. line 4688)
> "the column stats say non_null=442 for entity_id but there are 442 total rows.
> So ALL have non-null entity_id. This means the `case … concat(…)` returns
> non-null even on rollup rows…" (line 5884)

It never found the real cause (the rollup never made it into the SQL) and shipped
a passthrough form that fails.

## Root cause — one construct, reproducible with NO union join

`by rollup (…)` is materialized ONLY by stamping the grouping mode onto
**un-grouped projection aggregates**. When the SELECT projects pre-aggregated
values with NO fresh aggregate function, the spec attaches to nothing and is
dropped — **no ROLLUP/GROUPING SETS in the SQL, no error, wrong rows.**

`trilogy/parsing/v2/select_finalize.py`:
- `_propagate_select_grouping` (L995) — for a non-None `spec`, loops select items
  and calls `_collect_ungrouped_aggregates_deep`, stamping `wrapper.grouping =
  spec.mode` / `.grouping_sets` onto each hit (L1042-1045). **If the loop finds
  zero un-grouped aggregates, it stamps nothing and returns — the rollup spec is
  discarded with no diagnostic.**
- `_is_ungrouped_aggregate` (L929) — requires a bare `AggregateWrapper` with
  `not node.by`. A passthrough of a rowset/`auto` measure (already summed at its
  own grain) is not one, so it is never collected.

Downstream, `query_processor.py` builds a plain GROUP BY (no `rollup_concepts`),
so DuckDB gets `GROUP BY 1,2,…` with no subtotal rows.

## Trigger matrix (current working tree, workspace `20260703-212501`)

| # | Construct | `by rollup` in SQL? |
|---|-----------|:---:|
| M1 | base-model `select … sum(x) by rollup (a,b)` | **YES** (canonical q05 works) |
| M2 | `union join` two rowsets, PASSTHROUGH `coalesce(sale_agg.sales,0)` + rollup | **NO** — dropped |
| M3 | `union join` two rowsets, passthrough, no rollup | n/a |
| M4 | SINGLE rowset, PASSTHROUGH `coalesce(sale_agg.sales,0)` + rollup | **NO** — dropped (executes: 442 rows, **0 subtotal rows**) |
| M5 | SINGLE rowset, FRESH `sum(sale_agg.sales)` + rollup | **YES** |
| M6 | `union join` two rowsets, FRESH `sum(sale_agg.sales)` + rollup | **YES** |
| M7 | `auto tot <- sum(x) by …`, referenced as passthrough + rollup (no rowset/join) | **NO** — dropped |

Key result: the drop is **not** union-join-related. It is governed solely by
whether the final SELECT contains an **un-grouped aggregate**. Passthrough of an
already-aggregated rowset/`auto` column (M2/M4/M7) → dropped; wrapping the same
column in a fresh `sum(…)` (M5/M6) → preserved. The canonical `query05.preql`
dodges it because its `@windowed(…)` macro expands to fresh `sum(…)` in the final
SELECT (and uses no rowset), so M1 keeps the rollup and yields the reference rows
(verified: 100 rows incl. `(None,None,…)` grand total and `('catalog channel',
None,…)` subtotal, returns-only entities like `catalog_pageAAAAAAAAAABBAAAA`
present with 0 sales / 326.05 returns).

## Classification

- **PRIMARY: framework bug (silent wrong-result), the token-sink driver.** An
  explicit `by rollup (…)` clause is silently discarded when the projection has
  no un-grouped aggregate. It should EITHER roll up the passthrough measure
  (re-aggregate the pre-summed column) OR raise a clear error
  (`"by rollup requires an aggregate measure in the SELECT; a passthrough of a
  pre-aggregated column cannot be rolled up — wrap it in sum(…)"`). Silently
  emitting a plain GROUP BY with no subtotal rows and no error is what burned
  1.6M tokens: the agent saw the rows, saw no subtotals, and had no signal that
  the clause it wrote was ignored. The inconsistency (M5/M6 keep it, M2/M4/M7
  drop it) made it un-diagnosable.
- **Secondary / authoring (not the sink):** the submitted query also derives
  labels + `total_returns` from the SALE arm only (`sale_agg.*`), so returns-only
  entities and the correct return totals are wrong — the previously-diagnosed
  label/returns-coalesce authoring defect. Real, but subordinate to the rollup
  drop (which alone guarantees failure).
- **Not the cause:** no `union join` optimizer crash here (distinct from q59's
  `value_set_join_upgrade.py` regression); the `union join` itself resolves.

## Fix options (do NOT implement — READ-ONLY task)

- **Framework (preferred):** in `_propagate_select_grouping`
  (`select_finalize.py` L995), when `spec is not None` but zero un-grouped
  aggregates were stamped, raise a clear `InvalidSyntaxException` (as above), OR
  extend rollup materialization to re-aggregate passthrough measures at the
  rollup grain. Add a join-matrix / rollup cell: "`by rollup` over a passthrough
  of a pre-aggregated rowset/`auto` column."
- **Guidance (interim, prevents the sink):** the model/question should state that
  `by rollup (…)` requires the rolled measures to be **fresh aggregates in the
  final SELECT** — combine the rowsets and write `sum(sale_agg.sales)` (M6), not
  `coalesce(sale_agg.sales, 0)` passthrough. The agent-info rollup docs
  (conv. lines 654-683, 2824-2862) show only base-model `sum(x) by rollup (…)`;
  they never warn that passthrough silently no-ops.

---

## Re-run 20260704-035023 — 3,170,128 tokens, STILL FAILED (worse churn, DIFFERENT root cause)

**Verdict:** the original passthrough rollup-drop was **NOT fixed** (M7 still drops
below), but the agent **did not hit it this run** — it wrote the recommended
**fresh-`sum(…)` idiom (M6)**, so ROLLUP was emitted correctly. The failure + the
doubled 3.17M-token churn came from a **NEW, distinct defect pair**, not the rollup
drop. Net: q05 behavior *changed* — the framework rollup-drop was side-stepped by
better authoring, but the run got WORSE because the new failure is even less
diagnosable (the SQL now looks perfectly correct).

### Was ROLLUP emitted? (current engine, workspace `20260704-035023/workspace/`)

| Shape | ROLLUP/GROUPING SETS in SQL? | vs original doc |
|---|:---:|---|
| **Actual submitted `query05.preql`** (union rowset + FRESH `sum(combined.x)`) | **YES** — `GROUP BY ROLLUP (1,2)` | agent switched off passthrough → no drop |
| M6 (union rowset + fresh `sum`) | **YES** | unchanged (was YES) |
| M7 (`auto tot <- sum(x) by …` referenced as passthrough) | **NO — still silently dropped** | **UNCHANGED — bug still live** |

So the underlying passthrough-drop (`_is_ungrouped_aggregate` requires `not
node.by`, so a pre-`by`'d `auto`/rowset measure is never stamped —
`select_finalize.py` `_is_ungrouped_aggregate` L993 / `_propagate_select_grouping`
L1067) is **still present**. The battery of fixes reworked this area
(new `_collect_ungrouped_aggregates_deep` descends ConceptRefs but stops at rowset
boundaries) but did **not** close the passthrough hole. The agent simply avoided it.

### What actually drove the 3.17M tokens THIS run

The generated SQL is **correct** — `GROUP BY ROLLUP (1,2)` producing 700 rows
(696 leaves + 3 channel subtotals + 1 grand total). The failure is the **ORDER BY**:

```
order by _level asc,                         -- line 52 of query05.preql
    combined.channel_type asc nulls first,
    combined.entity_id asc nulls first
limit 100
```

renders to

```
ORDER BY grouping(channel_type) + grouping(entity_id) asc,  -- level 0 (leaves) FIRST
         channel_type asc nulls first, entity_id asc nulls first
LIMIT 100
```

`_level asc` sorts **leaves (level 0) first**, so the 100-row window is **100
catalog leaves** and NEVER reaches the level-1 subtotals or the level-2 grand total
→ result has **0 null-key rows**. The reference (`tests/modeling/tpc_ds_duckdb/
query05.preql`) orders by `channel_label NULLS FIRST, entity_id NULLS FIRST` (no
level), putting the grand total + subtotals **at the top** → they land inside the
top 100. Confirmed: deleting `_level asc,` from the submitted query yields exactly
the reference window — `[(None,None), ('catalog channel',None), <catalog leaves…>]`,
2 null-key rows in the first 100. `score_query(...,5,...)` = **fail, ref 100 /
cand 100, "result set differs"**; canonical `query05.preql` builds and yields the
100 reference rows on the current engine (it is what score_query runs for the ref).

**Two compounding defects:**

1. **AUTHORING / reasoning error (the primary):** the agent *deliberately* chose
   leaves-first ordering, reasoning (conv. 6552-6556, 7238-7247) *"leaves first
   (level 0), then subtotals per channel (level 1), then grand total (level 2)… ✓"*
   — not realizing that with `LIMIT 100` and 696 leaves, levels 1/2 fall outside
   the window. It then "verified" correctness against **un-limited** column stats
   (conv. 6558-6564, 6948: *"1 null channel, 4 null entity IDs … output looks
   correct"*) — those nulls exist in the full 700-row rollup but are cut by the
   ordered LIMIT. So the agent had a correct-looking ROLLUP + stats that "proved"
   subtotals exist, and shipped. This is why churn *doubled*: unlike the original
   run (where no ROLLUP was in the SQL, an eventually-findable signal), here every
   artifact the agent inspected looked right.

2. **GRAMMAR FOOTGUN (`--` is not a comment):** line 50 is
   `--grouping(combined.channel_type) + grouping(combined.entity_id) as _level`,
   which the agent believed it had *commented out*. Trilogy has no `--` line
   comment (comments are `#`). Empirically the `--`-prefixed select item is
   **excluded from the projection** (the final SELECT has 5 cols, no `_level`) but
   its **`as _level` alias is still registered as a resolvable concept** — so
   `order by _level asc` (line 52) silently binds to `grouping(ct)+grouping(eid)`.
   Verified: `order by _level` with the "commented" line present → resolves to the
   grouping expr; without any such line, or with a different name → clean
   `UndefinedConceptException`. The agent's attempt to disable the level column had
   no effect on the ORDER BY, cementing the leaves-first sort. This inconsistency
   (line suppressed from output yet its alias leaks into ORDER BY) is a real,
   separate framework footgun worth a diagnostic — but it is not the rollup drop.

### Classification of the 20260704 failure

- **Not a rollup-drop recurrence.** ROLLUP was emitted correctly; the original
  framework bug did not fire (fresh-`sum` idiom used).
- **Primary = authoring/reasoning:** `order by <grouping-level> asc` + `LIMIT 100`
  is wrong for a nulls-first top-N; guidance should say order subtotals/grand-total
  to the TOP (`… nulls first`, no ascending level) so they survive the LIMIT.
- **Secondary = grammar footgun:** `--` silently registers-but-suppresses a select
  alias that still resolves in ORDER BY. Candidate for a lint/diagnostic
  ("`--` is not a comment; use `#`").
- **Underlying passthrough rollup-drop = still open** (M7), just not exercised.

**Did the battery of fixes change q05?** Yes — *worse in tokens, unchanged in
outcome*. The passthrough rollup-drop is untouched/still live, but the agent
side-stepped it with fresh sums (the doc's own guidance), then failed on a new,
harder-to-diagnose ordering+LIMIT defect (aggravated by the `--` footgun), doubling
churn from 1.64M → 3.17M. The fix priorities are now (a) the ORDER-BY-level-vs-LIMIT
guidance and (b) the `--`-not-a-comment diagnostic, **in addition to** the
still-unfixed passthrough rollup-drop.
