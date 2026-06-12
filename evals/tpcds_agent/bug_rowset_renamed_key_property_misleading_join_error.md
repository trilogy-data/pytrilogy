# Bug: rowset that renames a dimension key → unsourceable property + misleading "scoped-join" error

**Status:** OPEN (found 2026-06-12, full-99 TPC-DS rebaseline, ingest q14).
**Severity: MEDIUM** — not a crash, but (a) a valid query fails to resolve, and
(b) the error tells the user to fix a `join` that doesn't exist. The prior
property-enrichment fix (`project_scoped_join_property_enrichment`) covered the
`join`-rename case only; the **rowset (`with … as`) rename case** is unhandled and
the error message is wrong for it.

## Symptom

```
UnresolvableQueryException: Could not resolve query. `channel` is a property of
`ss.item.class_id`, which is present only as the renamed scoped-join key(s)
qualifying.class_id. Add the base key to the join group, e.g.
`inner join qualifying.class_id = ... = ss.item.class_id`.
```

**There is no `join` in the query.** `qualifying` is a `rowset` (a `with … as`
named statement) that renames item dimension keys (`ss.item.class_id as class_id`).
The remediation the message suggests (`inner join qualifying.class_id = … =
ss.item.class_id`) is inapplicable — the query relates the rowset to the facts via
membership (`ss.item.class_id in qualifying.class_id`), not a join.

## Repro (deterministic)

`evals/_repros/q14_rowset_renamed_key_property.preql`:

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260612-133004_ingest/workspace')   # any raw-model ws
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(Path('evals/_repros/q14_rowset_renamed_key_property.preql').read_text())
# -> UnresolvableQueryException: ... present only as the renamed scoped-join key(s) ...
```

## Shape that triggers it

```sql
with qualifying as
where ss.date_dim.year between 1999 and 2001
  and count(cs.order_number ? ...) by ss.item.brand_id, ss.item.class_id, ss.item.category_id > 0
  and count(ws.order_number ? ...) by ss.item.brand_id, ss.item.class_id, ss.item.category_id > 0
select
  ss.item.brand_id  as brand_id,      -- renames (strips namespace) the dimension keys
  ss.item.class_id  as class_id,
  ss.item.category_id as category_id;

with channel_sales as union( ... arms filter `ss.item.class_id in qualifying.class_id` ... )
  -> (channel, brand_id, class_id, category_id, total_sales, total_number_sales);

select ... sum(...) by rollup channel, brand_id, class_id, category_id ...;
```

The rowset's `as brand_id` / `as class_id` strips the `ss.item.*` namespace, so the
group `{brand_id, class_id, category_id}` no longer contains `ss.item.class_id`.
A downstream concept that is a *property of* `ss.item.class_id` can't source from
the renamed key — same root as the join-rename case, but through a **rowset**.

## Two things to fix

1. **The error message is wrong for rowsets.** It hard-codes the scoped-`join`
   remediation (`inner join qualifying.class_id = … = ss.item.class_id`). When the
   renamed key comes from a `rowset`/`with … as` (no join in the query), the message
   must not tell the user to chain a join group — it should describe the rowset
   rename (or how membership/`in` relates the rowset back to the base key). Find the
   raise site that emits "present only as the renamed scoped-join key(s)" and branch
   on whether a scoped join actually exists.
2. **Ideally it should resolve.** Selecting a property alongside a renamed
   dimension key from a rowset is a reasonable pattern (the agent kept the base key,
   just stripped its namespace via `as`). Either source the property through the
   rowset, or — if genuinely unsupported — say exactly that, not "add a join".

## Context

Same q14 trace as `bug_union_rollup_crossmodel_recursion.md`
(`results/20260612-180707_ingest`, not polluted). The agent burned its full
75-iteration budget partly bouncing off this misleading message. Prior partial
fix: `project_scoped_join_property_enrichment` (join-rename case only).
