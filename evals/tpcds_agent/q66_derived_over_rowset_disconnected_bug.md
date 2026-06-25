# BUG: derived expression over a rowset output column → DisconnectedConceptsException in scoped-join query

## Severity
Medium-High — a hard error (not silently wrong, unlike the q78 OUTER bug), but it blocks a natural idiom and sent the q66 agent into a long thrash (q66 was the #3 most expensive enriched query, 2.62M tokens, 48 iters). The agent misdiagnosed it as "the join can't relate the rowsets" and burned iterations restructuring joins that were never the problem.

## Symptom
In a query that scoped-joins two or more rowsets, selecting a **raw** rowset output column works, but wrapping that same column in **any arithmetic/derived expression** (`* 2`, `/`, `+`, a CASE over it) makes discovery fail:

```
DisconnectedConceptsException: ... the requested concepts split into 2 disconnected
subgraphs: {all_months.month}; {local.r}. Are you missing a join or merge ...?
```

The derived concept (`local.r`) becomes an isolated node — it doesn't inherit the source rowset's grain/keys in the connectivity graph, so it can't be related to the other joined rowset.

## Minimal repro (2 rowsets, constant-key cross join — no left join needed)
Engine from `evals/tpcds_agent/results/20260623-145720/workspace`:

```trilogy
import raw.all_sales as s;
import raw.date as d;

rowset all_months <- where d.year = 2001
  select d.month_of_year as month, 1 as join_key;

rowset wh_groups <- where s.channel in ('WEB','CATALOG') and s.date.year = 2001 and s.warehouse.id is not null
  select s.warehouse.id as wh_id, s.warehouse.square_feet as w_sqft, 1 as join_key;

-- WORKS:
select wh_groups.w_sqft, all_months.month
inner join wh_groups.join_key = all_months.join_key;

-- FAILS (only change: `w_sqft` -> `w_sqft * 2 as r`):
select wh_groups.w_sqft * 2 as r, all_months.month
inner join wh_groups.join_key = all_months.join_key;
-- DisconnectedConceptsException: subgraphs {all_months.month}; {local.r}
```

## Scope / characterization (against the q66 3-rowset shape)
- Raw rowset columns (dims OR measures, coalesced or not): **OK**.
- Constant-derived select columns (`2001 as year`, `'DHL,BARIAN' as ship_carriers`): **OK** — so the agent's "constants break it" theory was wrong.
- A derived expression over a rowset output (`wh_groups.w_sqft * 2`, `sales_agg.monthly_sales / wh_groups.w_sqft`, `sales_agg.monthly_sales + all_months.month`): **FAILS** (DisconnectedConceptsException, or UnresolvableQueryException for the cross-rowset measure+dim form).
- Curiosity for the fixer: `concat(wh_groups.w_name, all_months.month::string)` as the *sole* output column resolved OK, while `wh_groups.w_sqft * 2` alongside other columns fails — the boundary is fuzzy, consistent with a grain/lineage-propagation gap for derived-over-rowset concepts rather than a clean all-or-nothing rule. The minimal repro above is the reliable trigger.

## Root-cause hypothesis
A `local.*` concept derived from a rowset output (`Derivation.ROWSET` parent) is not being assigned the source rowset's grain/keys when the connectivity graph is built, so it forms its own disconnected component. Raw rowset columns carry the rowset grain (here the constant `join_key`) and connect through the cross/scoped join; the derived wrapper drops that lineage. Look at how connectivity-graph node grain is computed for concepts whose lineage roots in a rowset select, in the scoped-join discovery path (same general area as the recent scoped-join work, but a different failure mode than the q78 OUTER bug — `evals/tpcds_agent/q78_three_source_outer_join_bug.md`).

## Repro harness
`/scratchpad/repro66.py` (A/B/C isolation), `repro66b.py` / `repro66c.py` (characterization), `repro66min.py` (minimal 2-rowset). Pattern:
```python
import sys; sys.path.insert(0,'evals'); from common import scoring
ws=Path('evals/tpcds_agent/results/20260623-145720/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(body)  # raises here
```

## Expected fix
A derived concept over rowset output(s) should inherit the source rowset's grain so it stays connected through the scoped join — selecting `expr(col)` must work wherever selecting `col` works. At minimum, the disconnection error should name the derived concept's rowset lineage so the failure is diagnosable.

## Workaround for the model/agent (until fixed)
Compute the derived expression as part of a single combined SELECT off one anchor rowset, or push the arithmetic into the rowset that produces the base column, rather than deriving across/over scoped-join rowset outputs in the final projection.
