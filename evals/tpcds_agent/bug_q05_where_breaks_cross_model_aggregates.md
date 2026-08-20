# Bug: WHERE over disconnected aggregate islands falls through to render sentinels instead of the clean resolution error

**Re-verified OPEN 2026-08-20 (`6bdb4d7b4`)** - the minimal repro below reproduces
verbatim, both sums sentineled.

Status: OPEN. Found in run `20260817-013108_ingest_deepseek_deepseek-v4-flash`,
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

## Root-cause leads

- Sentinels emitted at `trilogy/dialect/base.py:1642` / `base.py:1876`.
- The clean multi-island diagnostics that SHOULD fire (if planning is refused)
  already exist: "disconnected subgraphs" discovery error and the
  "WHERE input(s) cannot be related" resolution error - both observed working
  in the same run on other queries. The WHERE-plus-multi-island-aggregate path
  bypasses both checks and falls through to render.

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

Raise site: `trilogy/core/processing/nodes/base_node.py:284`. Same hygiene
class: a user-reachable planning failure must produce an authored-level error,
not an internal invariant message. Worth a guard/translation once the primary
bug's fix locus is understood (they may share the demand-shaping defect:
memory says missing join keys / missing parents = node construction got wrong
arguments; fix in discovery/demand, not late).
