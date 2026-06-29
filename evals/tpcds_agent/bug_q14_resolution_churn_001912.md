# q14 resolution churn — run 20260629-001912 (3.24M tokens)

TPC-DS q14 (cross-channel: tuples present in store+catalog+web 1999-2001, then
current-year-vs-historical-avg). The canonical `tests/modeling/tpc_ds_duckdb/query14.preql`
builds and generates SQL cleanly on the current engine, so the model is sound.
Two distinct framework-looking errors drove the agent's churn.

## HEADLINE — `Referenced table "<cte>" not found` is a real codegen bug

### Symptom (from agent_log.q14.jsonl)
- line 56:  `Referenced table "vacuous" not found! Candidate tables: "ss_item_items"`
- line 125: `Referenced table "scrawny" not found! Candidate tables: "ss_date_date"`
- line 152: same, `scrawny` / `all3_brand_id`

`generate_sql` SUCCEEDS (exit 0, valid-looking SQL) — the failure only surfaces at
DuckDB execution as a `BinderException`. Classic silent-bad-codegen.

### Agent construct
The agent encoded the (brand_id, class_id, category_id) 3-way INTERSECT as a
**scalar concat membership** whose RHS is an *expression over another rowset's columns*:

```
(ss.item.brand_id::string || '|' || ss.item.class_id::string || '|' || ss.item.category_id::string)
  in (store_bcc.brand_id::string || '|' || store_bcc.class_id::string || '|' || store_bcc.category_id::string)
```

### Minimal repro (workspace model, executed against tpcds.duckdb)
The concat is incidental — a single-column expression RHS is enough:

```trilogy
import raw.store_sales as ss;
with store_bcc as where ss.date.year between 1999 and 2001
  select ss.item.brand_id as brand_id;
with store_nov as
  where ss.date.year = 2001 and ss.date.month_of_year = 11
    and ss.item.brand_id::string in (store_bcc.brand_id::string)   -- RHS is an EXPRESSION
  select ss.item.brand_id as brand_id, sum(ss.quantity*ss.list_price) as total_sales;
select store_nov.brand_id, store_nov.total_sales;
```

generate_sql emits (note the bare CTE reference, no subselect, not in FROM):
```sql
... WHERE ... and cast("ss_item_items"."I_BRAND_ID" as string)
        in (cast("cooperative"."store_bcc_brand_id" as string))
```
`cooperative` is emitted as a top-level CTE but is **not joined into** the consuming
CTE and **not wrapped in a `SELECT ... FROM cooperative` subselect**. Execution:
`Binder Error: Referenced table "cooperative" not found! Candidate tables: "ss_store_sales"`.

### Isolation
- `ss.item.brand_id in store_bcc.brand_id` (RHS = **bare concept**) → EXEC OK (proper
  existence subselect emitted).
- `ss.item.brand_id::string in (store_bcc.brand_id::string)` (RHS = **expression**) → broken.

So the trigger is precisely: membership (`in`) where the RHS, after stripping
parentheses, is **not a `BuildConcept`** but a function/expression that references a
rowset/CTE column.

### Root cause — `trilogy/dialect/base.py` `render_expr`, SUBSELECT_COMPARISON branch
- `base.py:1540` — `if isinstance(right, BuildConcept):` is the ONLY path that consults
  `existence_source_map` / `source_map` and renders the existence subselect
  (`... in (select <col> from <target> where <col> is not null)`). This is why
  bare-concept membership works.
- `base.py:1614-1618` (List/Tuple/Parenthetical) and the **fallback at `base.py:1620`**
  render `render(left) <op> ( render(right) )`. When `right` is a `BuildFunction`
  (cast / `||` concat / arithmetic) over rowset columns, `render_expr` recurses and
  emits each inner `BuildConcept` as `"<source_cte>"."<col>"` inline — i.e. a bare
  reference to the existence CTE that is never joined or sub-selected. Result: dangling
  table reference.

Discovery *does* build the existence source (the CTE exists in the WITH list), but the
render layer only wires it up for the bare-`BuildConcept` case; any expression-typed RHS
escapes existence handling. Same family as the previously-fixed ROW_TUPLE composite
membership (`(a,b) in (set.a,set.b)`) and the q8 array-unnest fix — both special-case the
RHS node shape — but the scalar-expression-over-rowset RHS was never covered.

Classification: **(a) real framework bug.** A reasonable construct (`<expr> in (<expr over
rowset cols>)`) generates SQL referencing a non-emitted table. Headline.

## SECONDARY — `cannot merge all concepts ... N disconnected subgraphs`

### Symptom (lines 50, 95, 107)
e.g. `split into 4 disconnected subgraphs: {combined.brand_id, combined.category_id,
combined.channel, combined.class_id}; {local._level, local.brand_id, ...};
{local._virt_agg_grouping_...}; {local.group_sales}. Are you missing a join or merge?`

### Construct
Final SELECT over the `combined` union-rowset with: CASE-rewritten rollup keys
(`case when grouping(combined.channel)=1 then 'ALL' else combined.channel end`),
`by rollup (...)`, a projected `_level <- sum(grouping(...))` used in ORDER BY, and a
top-level grain-less `auto group_sales <- sum(combined.total_sales)` used in HAVING
alongside `grouping()`.

### Findings
Each ingredient builds fine in isolation against the same workspace model:
- clean grouping()+CASE+`by rollup` over the union rowset → GEN OK
- projected `sum(grouping(...)) as _level` + `order by _level` → GEN OK
- grain-less `auto group_sales <- sum(combined.total_sales)` in HAVING of a rollup → GEN OK
- a genuinely undefined ORDER BY ref → clean `UndefinedConceptException`
- a genuinely undefined HAVING ref → clean `InvalidSyntaxException` ("HAVING references
  'local.group_sales', which is not defined")

The disconnect only emerges from the **full combination** (union-rowset outputs
`combined.*` + CASE-derived `local.*` rollup keys + the `_virt_agg_grouping` virtual +
the separately-defined grain-less `group_sales` agg), and I could not reduce it to a
minimal snippet. The grain-less `auto group_sales` (a second aggregate node over the
union, distinct from the select's own `sum(combined.total_sales)`) is the most likely
culprit for the un-joinable `{local.group_sales}` island.

Classification: **(b) primarily agent-driven**, but with a genuine UX defect — the engine
reports "disconnected subgraphs / missing a join or merge" for what is a
grouping/rollup-over-union-rowset resolution gap, which is misleading and not actionable
(there is no join the author can add). Not the headline; not cleanly reproducible.

## Files
- bug: `trilogy/dialect/base.py:1540` (only-handles-BuildConcept), fallback `:1620`
- canonical (works): `tests/modeling/tpc_ds_duckdb/query14.preql`
- agent workspace: `evals/tpcds_agent/results/20260629-001912/workspace/query14.preql`
- agent log: `evals/tpcds_agent/results/20260629-001912/agent_log.q14.jsonl`
  (writes at lines 52/121/148 → Referenced-table errors 56/125/152; writes 46/91/103 →
  cannot-merge errors 50/95/107)
