# Bug: filtered-membership concept with a per-key aggregate condition → "GROUP BY clause cannot contain aggregates"

**Status:** FIXED 2026-06-07 (found enriched eval q94).
Fix in `trilogy/core/processing/node_generators/filter_node.py` `_can_pushdown_as_grouped_filter`:
the grouped-filter pushdown (content as group key, predicate → HAVING) is unsound when the filter
condition mixes per-key aggregate(s) with a non-aggregate predicate on **another filtered concept**
(`applicable_orders is not null`). The nested filter needs its own per-row CASE, which the grouped
form pushed into GROUP BY next to the aggregates (`GROUP BY CASE WHEN … count(…) …` — invalid) or
split the row predicate downstream and regrouped at the wrong grain (silently wrong results — e.g. a
faithful 2-source repro returned `[]` / 0 rows instead of the qualifying order). Now we return False
when any non-aggregate `row_argument` of the filter `where` is a `BuildFilterItem`, falling back to
the standard filter-node plan: per-key aggregates materialize in their own grouped CTE and the
membership CASE is evaluated at row grain. Tests:
`test_nested_filter_per_key_aggregate_membership_{not_in_group_by,executes}` in
`tests/engine/test_duckdb.py`. Minimal trigger: nested filter concept reference + ≥2 per-key
aggregates where one aggregates a LEFT-joined (partial-key) source.
**Severity:** high — the natural "keep orders where a PER-ORDER aggregate holds" filter
(`order ? count_distinct(warehouse) by order > 1`) generates SQL DuckDB rejects. `generate_sql`
succeeds; execution fails. q94 (TPC-DS correlated-exists) exhausted at 75 calls / 4.14M tokens.

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
LINE 7:     CASE WHEN count(distinct …) …
```
A per-key aggregate (`count_distinct(...) by <key>`, `bool_or(...) by <key>`) used inside an
inline-filtered key concept (`<key> ? <conditions-including-that-aggregate>`) is rendered into a
`GROUP BY`/`CASE`-in-group context → invalid.

## Trigger (verified, executes-fails on `raw.web_sales`)

The q94 shape — a filtered membership key whose condition nests another filtered concept plus two
per-key aggregate conditions:
```trilogy
import raw.web_sales as ws;

auto applicable_orders <- ws.order_number
  ? ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
    and ws.ship_address.state = 'IL'
    and ws.web_site.company_name = 'pri';

auto qualifying_orders <- ws.order_number
  ? applicable_orders is not null
    and count_distinct(ws.warehouse.id) by ws.order_number > 1     -- per-order aggregate
    and bool_or(ws.is_returned) by ws.order_number is not true;    -- per-order aggregate

where ws.order_number in qualifying_orders
  and ws.ship_date.date between '1999-02-01'::date and '1999-04-02'::date
  and ws.ship_address.state = 'IL'
  and ws.web_site.company_name = 'pri'
select ws.order_number as o, sum(ws.ext_ship_cost) as ship_cost, sum(ws.net_profit) as profit
limit 100;
```
→ `generate_sql` OK; executing the SQL against `tpcds.duckdb` raises the BinderException.

## Minimization status

NOT isolated. Stripped versions compile/run OK — a single per-key aggregate in a filtered
membership (`order ? count_distinct(...) by order > 1`) executes fine, and so does two of them.
The crash needs the FULL combination: a filtered key concept whose condition references **another
filtered concept** (`applicable_orders is not null`) **and** ≥1 per-key aggregate condition, used in
a top-level `in`. The nested-filtered-concept + per-key-aggregate + membership interaction is the
suspect. First task for whoever picks this up is to reduce it from the full q94 body (preserved in
`evals/tpcds_agent/results/20260607-225157/agent_log.q94.jsonl` — the inline `run --import
raw/web_sales:ws "<query>"` whose result contains "cannot contain aggregates").

## Suggested fix

A per-key aggregate used as a row/membership filter must be lowered as a grouped subquery / HAVING
on the per-key group, not projected into the outer GROUP BY. Same "aggregate ends up in GROUP BY"
family as `bug_aggregate_as_align_target.md` (FIXED) and the q70 grouping_id case (FIXED) — this is
the membership/filtered-concept manifestation.

## Companion issue from the same query (rowset field access) — NOT this bug — FIXED 2026-06-07

q94's agent also tried the rowset form and hit `Undefined concept: qualifying_orders.order_number`.
Root cause: a rowset column selected WITHOUT `as` keeps its FULL source path — `rowset qual <-
select ws.order_number …` exposes the concept as **`qual.ws.order_number`**, NOT `qual.order_number`
(verified: `as order_number` → `qual.order_number`). The agent used the SQL-CTE mental model
(reference the output by leaf name) and the undefined-error suggestion pointed at the *source*
(`['ws.order_number']`) instead of the real rowset path `qual.ws.order_number`, so it never found
the fix (add `as order_number`, or reference `qual.ws.order_number`). This is documented in the
`dedup-before-aggregate` agent-info example but was a recurring trap.

**Fixed:** the suggestion path now surfaces the staged rowset path. Two changes:
- `environment.py` `_find_similar_concepts` gained an `extra_keys` param (additional candidate
  addresses) plus self-exclusion of the looked-up address (a staged placeholder for the missing
  name was echoing itself).
- `select_finalize.py` `_staged_addresses(context)` collects everything visible to the parse via
  `context.concepts.values()` (pending + committed) and feeds it as `extra_keys` to both
  `_raise_undefined` and `raise_collected_undefined`. The rowset concept is only STAGED (not yet
  committed to `env.concepts`) at the membership-query parse, which is why the prior committed-only
  leaf-match missed it.

So referencing `qual.order_number` now yields suggestions including `qual.ws.order_number`. Covered
by `tests/test_undefined_concept.py` (`test_find_similar_extra_keys_surfaces_staged_concept`,
`test_find_similar_never_suggests_the_looked_up_address`,
`test_undefined_rowset_field_suggests_rowset_path`). The per-key-aggregate GROUP BY crash above is
still OPEN.
