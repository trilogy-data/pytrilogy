# q14 churn (run 20260629-214830, 1.75M tokens) — bug analysis

## Verdict on the prior expr-RHS membership fix: HELD

The prior bug (`<expr> in (<rowset.col>::string || ...)` → DuckDB
BinderException "Referenced table \"vacuous\" not found") is **gone**. Re-ran the
minimal repro on the current engine in the run's workspace (DuckDB, real
tpcds.duckdb):

```trilogy
import raw.all_sales as sales;
rowset combos <- where sales.date.year between 1999 and 2001
select sales.item.brand_id as brand_id, sales.item.class_id as class_id,
       sales.item.category_id as category_id
having count_distinct(sales.channel) = 3;

where sales.date.year = 2001 and sales.date.month_of_year = 11
  and (sales.item.brand_id::string || '|' || sales.item.class_id::string || '|' || sales.item.category_id::string)
      in (combos.brand_id::string || '|' || combos.class_id::string || '|' || combos.category_id::string)
select sales.channel as channel, sum(sales.quantity * sales.list_price) as total_sales
order by channel asc;
```

Runs cleanly, 3 rows (CATALOG 235070713.24 / STORE 320226392.86 / WEB 119438599.19).
No dangling-CTE binder error. The fix held.

(Note: the report named in the task, `bug_q14_resolution_churn_001912.md`, does
not exist on disk. The original failure is documented inside
`results/20260629-001912/agent_log.q14.conversation.txt` around msg 37 / lines
3474–3559, which is what this repro reconstructs.)

## What actually drove THIS run's 1.75M-token churn

q14 in this run **submitted a query that runs (exit 0, 100 rows) but FAILS**:
`report.json` → `"status":"fail", "detail":"result set differs from reference"`.
So this is **silent wrong rows**, not a crash. The token sink is the agent
thrashing against three things, the dominant one being a correctness bug it
could not see through:

### PRIMARY (silent wrong rows): scoped `inner join` onto a rowset renders as a `FULL JOIN`

The agent expressed the 3-channel intersect as a multi-key scoped join:

```trilogy
inner join sales.item.brand_id = qualifying_combos.brand_id
  and sales.item.class_id  = qualifying_combos.class_id
  and sales.item.category_id = qualifying_combos.category_id
```

Generated SQL for the submitted `query14.preql` (CTE `sparkling`) lowers this to:

```sql
FROM "young"
FULL JOIN "yummy"
  ON "young"."qualifying_combos_brand_id" = "yummy"."sales_item_brand_id"
 AND "young"."qualifying_combos_category_id" is not distinct from "yummy"."sales_item_category_id"
 AND "young"."qualifying_combos_class_id"   is not distinct from "yummy"."sales_item_class_id"
```

`FULL JOIN` has **OUTER** semantics: November-2001 sales whose
(brand,class,category) is **not** a qualifying combo are retained (their keys
survive via `coalesce(young.k, yummy.k)`), and qualifying combos with no Nov-2001
sale are retained with NULL measures. The authored `inner` restriction is lost,
so non-qualifying rows leak and rollup levels show NULL measures (the agent
observed at msg 27: "ran with 100 rows but only CATALOG, no STORE/WEB, no rollup
subtotals").

Minimal proof (inner-join form vs. the correct membership form, same data):

| channel | inner-join total | membership total (correct) | inner n | correct n |
|---------|------------------|----------------------------|---------|-----------|
| CATALOG | 235866054.30     | 235070713.24               | 46689   | 46536     |
| STORE   | 321366182.34     | 320226392.86               | 86880   | 86568     |
| WEB     | 119875361.11     | 119438599.19               | 23524   | 23438     |

Inner-join totals are uniformly inflated → leaking non-matching rows. A reduced
repro that does **not** project the join keys is even worse: the entire
`inner join combos` clause is **pruned from the SQL** (no filter at all). Both
manifestations come from the same root.

**Root cause (file:line):** scoped INNER-join-onto-rowset is intentionally
lowered to the identity+pseudonym consolidation path rather than key
substitution — `BuildEnvironment.scoped_rowset_inner_sources` and the
INNER pseudonym wiring in `trilogy/core/models/build.py:2387-2437`. That path
reuses the merge-node coalesce machinery, which emits a **FULL** join
(`MergeNode.create_full_joins`, `trilogy/core/processing/nodes/merge_node.py:231-250`,
`join_type=JoinType.FULL`; sibling forced-FULL registry in
`get_join_type`, `trilogy/core/processing/join_resolution.py:117-123`). The
author's `JoinType.INNER` is never re-applied as a restriction after the
coalesce, so the result is a FULL join that keeps non-matching rows.

This is the same family as the MEMORY note "FIXED q14 multi-key scoped join onto
rowset — INNER-onto-rowset uses identity+pseudonym not substitution so rowset
WHERE applies": that fix made the rowset WHERE materialize correctly, but the
join it produces is FULL, not INNER — so the *filtering* semantics regressed into
correctness-wrong rows. Canonical `tests/modeling/tpc_ds_duckdb/query14.preql`
sidesteps this entirely by using membership (`tuple_key in cross_tuples.ci_tuple_key`)
instead of a scoped join, which renders as a semi-join and is correct.

### SECONDARY (token sink, friction): `select ... <agg> as a by <grain>` → cryptic "expected JOIN_TYPE"

`select sales.channel, count(sales.order_id) as cnt by sales.channel;` (msg 28)
errors with `expected metadata, limit, order_by, where, having,
select_grouping, or JOIN_TYPE ... count(...) as cnt ??? by sales.channel`. A
select-level `by <grain>` after an aliased aggregate is rejected, and the error
points at `JOIN_TYPE`, sending the agent down a long dead end about join syntax.

### SECONDARY (silent, masked the failure): name defined only in a comment resolves in HAVING

The submitted query has the qualifying-combos metric **commented out** yet
referenced in HAVING:

```trilogy
select sales.item.brand_id as brand_id, ...,
    --count_distinct(sales.channel) as channel_count
having channel_count = 3
```

This **does not error** — `channel_count` (a name that appears only inside the
`--` comment) resolves and the HAVING is honored (generated SQL shows
`HAVING count(distinct sales_channel) = 3`). A genuinely-undefined name
(`having totally_made_up_xyz = 3`) errors correctly with
"HAVING references ... which is not defined", and a phantom name placed only in a
commented select item (`--... as zzz_phantom_metric`, `having zzz_phantom_metric
= 3`) also runs clean. So a commented-out `<expr> as <name>` still registers
`<name>` as a referenceable concept — the `--` comment body is being scanned for
aliases. This masked the agent's editing mistake and let a half-edited file
"validate", reinforcing the false belief that the query was correct.

## Classification

- PRIMARY: **correctness / silent-wrong-rows** — scoped INNER join onto a rowset
  emits a FULL join; non-matching rows leak, rollup measures go NULL. HIGH
  severity (passes validation, wrong answer). `build.py:2387-2437` +
  `merge_node.py:231-250` / `join_resolution.py:117-123`.
- SECONDARY-A: **confusing error** — select-level `by <grain>` after aliased
  aggregate → misleading "expected JOIN_TYPE". Token-sink contributor.
- SECONDARY-B: **silent acceptance** — a name present only in a `--` comment
  resolves as a concept in HAVING (should be undefined). Masking bug.

## Status of the prior fix

The expr-RHS membership "Referenced table not found" fix **HELD** (verified by
re-running the minimal repro — clean, correct rows). The 1.75M churn is a
different, still-open bug: scoped INNER-join-onto-rowset renders FULL.

Do NOT fix (per task).
