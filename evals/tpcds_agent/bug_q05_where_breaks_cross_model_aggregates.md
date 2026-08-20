# Bug: WHERE over disconnected aggregate islands falls through to render sentinels instead of the clean resolution error

**FIXED 2026-08-20.** Both the primary sentinel fall-through and the q06
internal-error leak now refuse cleanly. See "Resolution" below for the fix loci
and the one shape that remains genuinely unsupported.

Status: was OPEN. Found in run `20260817-013108_ingest_deepseek_deepseek-v4-flash`,
q05 (`probe_store.preql`). Minimal repro in hand. A related internal-error leak
from the enriched leg (q06) is appended as a secondary finding.

## Symptom

Summing measures from two unrelated models in one select PLANS AND RENDERS
fine (a cross join of two one-row aggregates). Adding a WHERE clause - even one
touching only ONE of the models - makes every measure lose its source and the
render dies on `INVALID_REFERENCE_BUG` sentinels:

```
Could not render the query: Missing source reference to ss.ext_sales_price;
Missing source reference to ss.net_profit; Missing source reference to
sr.return_amt; Missing source reference to sr.net_loss. ...
```

## Minimal repro

```python
from trilogy import Dialects, parse

MODEL = """
key sale_id int;
property sale_id.amt float;
property sale_id.sdate date;
datasource sales ( id: sale_id, a: amt, d: sdate ) grain (sale_id) address sales_tbl;
key return_id int;
property return_id.ramt float;
property return_id.rdate date;
datasource returns ( id: return_id, r: ramt, d: rdate ) grain (return_id) address returns_tbl;
"""
env, _ = parse(MODEL)
ex = Dialects.DUCK_DB.default_executor(environment=env)
ex.generate_sql("select coalesce(sum(amt),0) as s, coalesce(sum(ramt),0) as r;")
# OK - cross join of two single-row aggregates
ex.generate_sql(
    "select coalesce(sum(amt),0) as s, coalesce(sum(ramt),0) as r "
    "where sdate > '2000-01-01'::date;"
)
# ValueError: Missing source reference to local.amt; Missing source reference to local.ramt
```

## Trigger matrix

| Shape | Result |
|---|---|
| sums from two unrelated models, no WHERE | OK (two singleton aggregates cross-joined; well-defined) |
| + WHERE on model A's property only | FAIL (sentinel, BOTH sums lost) |
| + WHERE on both models' properties (AND) | FAIL (sentinel) |

## What SHOULD happen

These WHERE shapes are author errors, not plannable queries: the top-level
WHERE defines the row population for the whole select, and with disconnected
islands there is no single row universe to filter. Silently filtering only
island A while leaving island B's aggregate unfiltered would be a wrong-rows
footgun, and the framework already rejects this category elsewhere with a
clean, actionable error - the SAME RUN produced it on enriched q01:

```
WHERE input(s) ['ss.return_store.state'] cannot be related to the query
outputs [...]: no join or merge connects the filter's source to any
output-producing source. Add a join/merge relating them, or select a concept
from the filter's model.
```

The per-source-filter intent belongs to per-arm scoping (union TVF arms,
inline `sum(x ? cond)` filters), and that clean error is what teaches an agent
to get there.

The bug: the aggregates-only shape (every island collapses to one row, no
shared dims requested) bypasses whatever relatability check produces that
error, plans anyway, and dies at render with `INVALID_REFERENCE_BUG`
sentinels. Note the check's current wording would not catch the
WHERE-on-one-model case anyway ("no join or merge connects the filter's source
to any OUTPUT-PRODUCING source" - here the filter's source IS output-producing
via sum(amt); the disconnect is between the filter and the OTHER island's
aggregate). The fix needs a guard along the lines of: every output must be
connectable to the filtered population, or every island carrying an output
must be reachable from the WHERE inputs.

## Field occurrence

`results/20260817-013108_ingest_deepseek_deepseek-v4-flash/agent_log.q05.jsonl`,
`probe_store.preql`: sums over `root.store_sales` + `root.store_returns` with a
date+store filter on each, all four measures sentineled.

## Resolution

Two defects, two loci.

**1. The plan was never refused.** With the WHERE present, the condition prunes
the other island's datasource, so `gen_select_merge_node` correctly declines the
ROOT group. Its dependent AGGREGATE groups then built anyway with `parents=[]`,
producing GroupNodes with no source for `amt`/`ramt`. The post-assembly guard
that exists for exactly this (`_has_unsourced_leaf`) missed them: it only flagged
a parentless leaf whose outputs are `Derivation.ROOT`, and an aggregate OVER a
root is not itself root. Widened to ask the real question - can this leaf render
from literals alone (`literal_producible`) - which covers both.

Note the near-miss: pruning those unsatisfiable outputs earlier, inside
`satisfiable_outputs`, looks like the tighter fix and is wrong. The bogus
parentless node is load-bearing; it has to survive to the assembled tree so the
disconnected-subgraph diagnostics can run on it and say WHICH concepts split.
Pruning it early leaves a partial plan that sentinels the condition args instead
(caught by `tests/test_incomplete_condition_disconnected.py`, whose shape depends
on it). The free pass now carries a comment saying so.

**2. No check could name the split.** `_crossjoinable` skips single-row outputs,
so `sum(amt)`/`sum(ramt)` were invisible and only `sdate` remained - one
component, no split, nothing to report. Added
`raise_if_where_population_split` (`discovery_utility.py`), a POST-FAILURE
refiner wired into `_plan_query_node`: it anchors the skipped single-row outputs
by their upstream row sources and names which outputs the WHERE cannot restrict.
Post-failure only, so it can sharpen a message but never reject a query that
plans.

The repro now gives:

```
WHERE input(s) ['sdate'] cannot restrict output(s) ['r']: no join or merge
relates the filter's source to the source of those outputs, so the WHERE has no
single row population to define -- the outputs would cross-join in unfiltered.
Add a join/merge relating them, or scope the filter to the source it belongs to
with an inline filtered aggregate (e.g. `sum(x ? <condition>)`).
```

Regression: `tests/core/processing/test_where_over_disconnected_aggregate_islands.py`
(the failing shapes plus controls for the shapes that must keep working - no-WHERE
cross join, single-island WHERE, constant beside a WHERE, the inline-filter
spelling, and literal-producible outputs under a parentless group).

## Sibling shape inherited from the closed q17 handoff

`handoff_composite_membership_invalid_reference_q17.md` was deleted on 2026-08-19
once its primary bug was fixed (tuple membership inside an inline-filtered
aggregate now plans off the same fact-anchored island as the plain-`where` path;
regression `tests/modeling/tpc_ds_duckdb/test_q17_composite_membership.py`). Its
open follow-up belongs to THIS defect class and is recorded here so it is not
lost: a **foreign-fact ROW predicate inside a filtered aggregate** was observed
rendering `INVALID_REFERENCE_BUG` sentinels rather than the clean
disconnected-subgraph error. It was not re-reproduced on 2026-08-20 - a
synthetic two-fact model with a shared customer key plans it cleanly, so the
shape needs the tpc_ds topology where the foreign fact is genuinely
unconnectable. Reconstruct it against `tests/modeling/tpc_ds_duckdb` before
treating it as separate from the repro above; the two most likely share one fix.

## Secondary finding: internal error leak (enriched q06)

`results/20260817-013108_enriched_deepseek_deepseek-v4-flash/agent_log.q06.jsonl`,
`probe6.preql` surfaced a raw internal invariant to the agent:

```
Unexpected error in probe6.preql: Invalid input concepts to node!
['cat_avg.category'] are missing non-hidden parent nodes; have
{'cat_avg.avg_price', 'item.category'} and hidden {'cat_avg.category'} ...
```

Raise site: `trilogy/core/processing/nodes/base_node.py:284`.

**FIXED 2026-08-20, and it does NOT share the primary's defect.** Minimal repro:
a rowset `cat_avg` grouped by category, then an inline subquery correlating back
to the enclosing scope (`(select cat_avg.avg_price where cat_avg.category =
category)`). `_filter_arg_parents` picks a built group to supply a FINAL-deferred
filter's row arg but never checked whether that group HIDES it; the merge reads
parents' `usable_outputs`, finds nothing, and raises the internal invariant. It
now unhides the column it selected that group to supply.

The shape itself stays unsupported, and the error it now reaches says why: the
keyless-join guard reports that the join axis was lost. That is accurate -
`cat_avg.category = category` is a join predicate, but the two sides carry
different addresses, so the merge's auto-join finds no shared key and would cross
join. Per `project_inline_subquery_implicit_rowset`, an inline `(select ...)` body
is a grain-less scalar that cross-joins; correlation was never in that design.
Making it work means teaching the merge to treat a condition equality as a join
axis, which is a feature, not this bug.

Characterized shapes (`tests/core/processing/test_correlated_inline_subquery_error_hygiene.py`):

| Shape | Before | After |
|---|---|---|
| inline subquery, no correlation | OK | OK (unchanged) |
| inline subquery correlated to outer scope | internal `Invalid input concepts to node!` | keyless-join guard, typed |
| same thing spelled as a direct rowset handle | clean `DisconnectedConceptsException` | unchanged |
| `avg(price) over (partition by category)` | OK | OK (unchanged) |

Caveat worth knowing before building on it: the unhide branch has ZERO firings
across the 144-query corpus (tpc_ds, tpc_h, gcat, faa, the_look), so the
byte-identical corpus A/B is not evidence about it. Only its own test exercises it.
