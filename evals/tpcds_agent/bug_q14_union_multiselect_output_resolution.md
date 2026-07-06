# q14 token sink — composite `subset join` onto a union-derived rowset → `could not resolve union/multiselect output` (framework bug)

Run: `evals/tpcds_agent/results/20260706-135542_enriched` (enriched leg, q14: 2.58M tokens, FAILED).
sql_bare passed q14, so raw SQL is fine — obstacle is Trilogy-side.

## Symptom

The agent's natural way to express q14's multi-column cross-channel intersection was:
1. `union(...)` the (brand_id, class_id[, category_id]) tuples of the 3 channels with a channel tag → rowset `all_combos`;
2. a derived rowset `qualifying_bcc` that RE-PROJECTS those union columns (`all_combos.b as brand_id, all_combos.c as class_id`) under a `count_distinct(ch) by (b,c) = 3` HAVING;
3. filter the Nov-2001 per-channel aggregate against the qualifying set with a **composite `subset join`** on both key columns.

Step 3 crashes:

```
Internal planner error: could not resolve union/multiselect output 'local._all_combos_c'
against CTE 'questionable' (it is not in that CTE's outputs
['nov_data.brand_id','nov_data.class_id','nov_data.total_sales',
 'qualifying_bcc.brand_id','qualifying_bcc.class_id']).
If this came from an ORDER BY on a union column, order by the projected output column instead.
```

The hint ("ORDER BY on a union column") is misleading — there is no ORDER BY involved. The agent burned the token budget iterating around this, eventually falling back to the concat-`in` idiom the canonical uses (see "Silent wrong result" below).

## Minimal repro

`MINL` — composite `subset join` between two rowset CTEs whose RHS re-projects a `union(...)`. No rollup, no HAVING, no ORDER BY needed:

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
with all_combos as union(
    (select ss.item.brand_id as b, ss.item.class_id as c),
    (select cs.item.brand_id as b, cs.item.class_id as c)
) -> (b, c);
with qualifying_bcc as
select all_combos.b as brand_id, all_combos.c as class_id;   -- re-projects union cols
with nov_data as
where ss.date.year = 2001
select ss.item.brand_id as brand_id, ss.item.class_id as class_id, sum(ss.quantity) as total_sales;
select
    nov_data.brand_id as brand_id, nov_data.class_id as class_id, sum(nov_data.total_sales) as q,
subset join nov_data.brand_id = qualifying_bcc.brand_id
subset join nov_data.class_id = qualifying_bcc.class_id
limit 100;
```

Run against the run's workspace:

```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('evals/tpcds_agent/results/20260706-135542_enriched/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
print(eng.generate_sql(open('MINL.preql').read())[-1])   # raises the RuntimeError
```

The run's own `workspace/tpcds.duckdb` — canonical `tests/modeling/tpc_ds_duckdb/query14.preql`
(with `import all_sales` → `import raw.all_sales`) generates + runs fine (100 rows), so the engine
and DB are healthy; the defect is specific to the construct below.

## Trigger matrix (one ingredient toggled at a time)

| # | RHS of subset join | # join keys | LHS of join | rollup/having/where-not-null | result |
|---|---|---|---|---|---|
| canonical | concat single key + `in` | 1 (concat) | rowset | rollup+having | OK, 100 rows |
| r1 (agent shape) | union-reproject rowset | 2 | union rowset | rollup+having+wnn | **FAIL** |
| rA | union-reproject rowset | 2 | union rowset | none | **FAIL** |
| rE | union-reproject rowset | 2 | union rowset | none (no wnn) | **FAIL** |
| rB | union-reproject rowset | 1 | union rowset | wnn | OK |
| rD | union-reproject rowset (b,c) | 1 | union rowset | wnn | OK |
| rF | PLAIN rowset | 2 | union rowset | none | OK |
| rG / MINL | union-reproject rowset | 2 | rowset | none | **FAIL** |
| MINH | union-reproject rowset (+having) | 2 | **base fact** | none | OK |
| rMin | **union directly** (no reproject) | 2 | base fact | none | OK |
| rMinG | union-reproject rowset | 2 | **base fact** | none | OK |

Isolated conclusions:
- **Composite (2+) join key is required** — single key never fails (rB, rD). Single key lowers to an `IN`/semijoin that keeps the union node reachable; composite forces a real merge/join CTE.
- **RHS must be UNION-derived** — a plain-rowset RHS never fails (rF), even with a union LHS.
- **RHS must RE-PROJECT the union** through an intermediate rowset — joining directly onto the `union(...)` rowset works (rMin). The failure needs the `qualifying_bcc.col` pseudonym layer over `_all_combos_c`.
- **LHS must be a rowset/derived CTE**, not the base fact — MINH/rMinG (base-fact LHS) pass; MINL (rowset LHS) fails. With a base-fact LHS the union column is still sourceable in the fact scan; with a rowset LHS the join CTE only carries the re-projected alias.
- rollup, HAVING, `where … is not null`, and `nov_data` being a union are all **irrelevant** (each toggled off with no change).

Minimal failing combination: **a composite `subset join` between two rowset CTEs, where the right rowset re-projects the columns of a `union(...)`.**

## Root cause (file:line)

Render path, DuckDB dialect:

- `trilogy/dialect/base.py:1150` — `_render_concept_sql`: `if c.lineage and cte.source_map.get(c.address, []) == []:` — the join CTE (`questionable`) lists `qualifying_bcc.class_id` in `output_lcl`, but its **`source_map` has no entry for that address** (the composite subset-join CTE keys the column under the collapsed canonical union address `_all_combos_c`, not the rowset alias). So the guard is entered and the concept is rendered *from lineage* instead of as a plain column ref.
- `trilogy/dialect/base.py:1197-1198` — the concept is a `BuildRowsetItem`, so it renders `c.lineage.content` — i.e. it dereferences the rowset handle `qualifying_bcc.class_id` down to its content, the union output column `_all_combos_c`.
- `trilogy/dialect/base.py:1213` → `trilogy/core/models/build.py:1917 find_source` → raises at `build.py:1929`: the union column can only be resolved at the union node's CTE, but we are rendering CTE `questionable`, whose align items don't map → `RuntimeError`.

Call chain (from traceback): `render_cte` (base.py:2244) → `_render_concept_sql` (1290 BASIC) → `render_expr` (1899) → `_render_concept_sql` (1198 rowset deref) → `_render_concept_sql` (1213) → `find_source` (build.py:1929).

The real defect is upstream of the render: the **composite subset-join CTE's `source_map` loses the union-column↔rowset-alias pseudonym** for the re-projected key concepts, so the renderer can't source `qualifying_bcc.class_id` directly and wrongly dereferences to the union node. The single-key path avoids it (semijoin keeps the union reachable); the composite path builds a merge/join CTE that drops the mapping. This is the same family as the pseudonym-collapse issues in prior q14/q02/q64 join work.

## Classification

- **Errors 1 & 2** (`could not resolve union/multiselect output … against CTE …`, vs CTEs `fabulous`/`yellow`): **FRAMEWORK BUG.** Reproduced minimally (MINL). This is the primary token sink — the composite `subset join` is the idiomatic Trilogy expression of q14's multi-column cross-channel intersection, and it is silently unavailable (crashes as an "internal planner error") whenever the membership set is a re-projected `union(...)`. The misleading "ORDER BY on a union column" hint also cost the agent iterations.
- **Error 3** (`Resolution error … Could not resolve connections for query with output ['local.total…','local.cnt…']`): **not a framework bug** — the agent wrote `sum(ss…) + sum(cs…) + sum(ws…)` over three independent fact tables in one select with no join/merge; that is genuinely disconnected and the engine is right to reject it (guidance-level, message is reasonable).

## Silent wrong result (secondary)

The agent's *final accepted* `workspace/query14.preql` (concat-key `in` idiom, the canonical approach) generates + runs (100 rows) but returns **different totals** than the canonical answer:

- agent grand total: `674,735,705.29 / 156,542`
- canonical grand total: `673,409,655.64 / 155,567`

Cause: the agent applies the average-threshold HAVING at the **rollup grain**
(`having sum(filtered.total_sales) > sum(threshold.thresh) * sum(sale_count)/sum(sale_count)`),
whereas the reference filters at the **pre-rollup per-(channel,brand,class,category) L0 grain**
(`bucket_sum_l0 > avg_sales.average_sales`) before rolling up. This is a grain/modeling choice,
not a framework silent bug — but the token bar (2.58M) is what surfaced it, since the wrong-grain
filter changes which leaf groups survive into the rollup. The agent only landed on this
imperfect form after the framework bug above blocked the cleaner composite-subset-join expression.

## Suggested follow-up (not done here)

Fix the composite subset-join CTE `source_map` to carry the union-column↔rowset-alias pseudonym so
`qualifying_bcc.class_id` sources directly (base.py:1150 guard would then be false and 1197 deref
avoided). Until then, guidance: express multi-column intersection membership with a single concat key
+ `in` (as canonical does), not a composite `subset join` onto a re-projected `union(...)`.
