# Filtered count of a KEY: dedup GroupNode drops the filter virtual, union render emits phantom `_virt_filter_*` columns

Re-verified: 2026-08-20 against working tree (`c40ef023b`), all repros below run fresh.

## Symptom

q14 enriched run `results/20260820-031800_enriched_deepseek_deepseek-v4-flash` (567k tokens): the
agent's structure probe died with a db-level error from generated SQL:

```
(_duckdb.BinderException) Binder Error: Values list "cheerful" does not have a column named
"_virt_filter_sk_1987727915913391"
```

The rendered SQL puts a 3-arm UNION ALL CTE (`cheerful`, the merged `all_sales` partial
datasources) that projects ONLY the grain keys, then a downstream CTE selects
`"cheerful"."_virt_filter_sk_*"` columns no arm ever produced.

Same defect on a single-table model surfaces as the clean generation error instead:

```
ValueError: Could not render the query: Missing source reference to ss.ticket_number.
```

## Minimal repros

Union / BinderException (any fresh engine on the run workspace, e.g. via
`scoring.make_scoring_engine(ws/"warehouse.duckdb", ws, "tpcds")`):

```sql
import raw.all_sales as s;
select count(s.item.sk ? s.quantity is not null) as c;
```

Self-contained, no workspace, 15 lines (clean-error branch of the same defect):

```sql
key tick int;
key isk int;
properties <tick, isk> (
    qty int,
);
datasource t (
    a: tick,
    b: isk,
    q: qty,
)
grain (isk, tick)
address tbl;

select count(isk ? tick > 0) as c;
-- GEN-FAIL: Missing source reference to local.tick
```

## Trigger matrix

The trigger is a filtered aggregate whose ARGUMENT IS A KEY (`count(key ? cond)`), which by
design counts over the key's distinct domain and therefore forces a dedup of the input
stream to the key's grain. Verified fresh-engine per cell:

| model shape | query shape | result |
|---|---|---|
| union model (`raw.all_sales`) | `count(item.sk ? <any cond>)`, bare or beside channel / `count(grain(...))` / other aggs; cond on key or property | EXEC-FAIL BinderException (phantom `_virt_filter_*` on the union CTE) |
| single table, 2 local keys `<tick, isk>` | `count(isk ? tick > 0)` (cond on the co-grain key) | GEN-FAIL Missing source reference |
| single table, 2 local keys | `count(isk ? qty > 0)` (cond on property), alone or beside other aggs | OK |
| single table, imported dim key `<tick, item.sk>` | `count(item.sk ? tick > 0)` | GEN-FAIL |
| single table, imported dim key | `count(item.sk ? qty > 0)` beside `count(qty)` or `count(grain(...))` | GEN-FAIL |
| single table, imported dim key | `count(item.sk ? qty > 0)` alone, or two filtered counts | OK |
| any | `count(qty ? qty > 0)` / `sum(qty ? cond)` (filtered PROPERTY arg) | OK |
| any | plain `where` clause instead of inline filter | OK |
| any | `count(item.sk ? cond)` grouped by an imported dim attribute | OK |
| full workspace `raw.store_sales` | `count(ss.item.sk ? <cond on anything>)` beside a second aggregate | GEN-FAIL (this is why the eval agent's every structure probe on real models hits it) |

## Root cause

Chain of four cooperating pieces; the demand hole is (3), per doctrine the fix belongs there.

1. `trilogy/parsing/common.py:1189` (`filter_item_to_concept`): a filtered KEY builds the
   virtual with empty grain and `keys = {content key}` (deliberate: a key's filtered count
   ranges over the key's domain). `_aggregate_input_grain`
   (`trilogy/core/processing/v4_helper/concept_graph.py:910`, derived-key branch ~:953)
   therefore sets `aggregate_input_grain = {key}`, so the aggregate needs a dedup of its
   input stream to the key's grain, with the filter mask computed BELOW that dedup.

2. `trilogy/core/processing/v4_helper/strategy_builder.py:622` (`_parent_nodes_for`,
   `row_preserving_input` at :707, `_group_renderable_from` at :1190): the FILTER group is
   inlined away as the aggregate's parent because the mask is re-renderable from the root's
   outputs (content + condition columns all present). Plan intent: re-derive the CASE mask
   inline at the aggregate.

3. `trilogy/core/processing/v4_helper/strategy_builder.py:4067-4141` (aggregate input
   normalization in `build_strategy_node`): inserts the dedup GroupNode and has a widen
   fallback (:4118-4133) for exactly this case ("a filter virtual the plan meant to
   re-derive at the aggregate itself must be computed BELOW the dedup"). Two holes:
   - It gates on `concept_satisfiable(candidate, parent_output_addresses(parent))`, and
     `parent_output_addresses` (`v4_helper/projection.py:10`) returns the outputs of the
     parent's PARENTS. For a leaf SelectNode bound straight to a datasource that set is
     EMPTY, so the check fails even though the datasource carries every needed column.
   - When no parent matches, the loop falls through SILENTLY: the aggregate argument is
     simply dropped from `normalize_concepts`, so the dedup GroupNode projects only the
     key and the condition columns are grouped away. No error at plan time.

4. Renderer divergence on the broken plan:
   - Single-table: the aggregate CTE inline-expands the virtual, hits a condition column
     with no source, and raises the clean `Missing source reference` ValueError
     (`trilogy/dialect/base.py:1884` via the `INVALID_REFERENCE_STRING` path).
   - Union: when the widen check passes, `widen_projection` (`projection.py:135`) appends
     the virtual to the union NODE's output list only; nothing propagates it into the
     per-arm children, so the UnionCTE claims a column no arm computes.
     `hide_unused_concepts` then hides the condition column in the arms (nothing visible
     consumes it), and the renderer's union escape hatch at `trilogy/dialect/base.py:1642`
     ("unions won't have a specific source mapped; just use a generic column reference")
     emits the bare `safe_address` instead of raising. Result: the phantom
     `"cheerful"."_virt_filter_*"` reference and the BinderException, i.e. the union path
     converts what should be the clean error of (4a) into invalid SQL shipped to the db.

Plan-level evidence (pre-compile, correct query text): the UnionCTE's `output_columns`
include `local._virt_filter_sk_*` and `s.quantity`, but every `internal_ctes` arm lists
only `[channel, item.sk, order_id, quantity]` with `hidden_concepts =
{channel, order_id, quantity}`; the downstream group CTE source-maps the virtual to the
union. Compilation of that exact ProcessedQuery object reproduces the bad SQL.

## Rest of the 567k churn (checked, no second engine bug)

The run's only hard error is this BinderException (2 occurrences of the same statement).
The remaining churn is the agent misreading `count(key ? cond)` DISTINCT-KEY semantics as
row counts: probe1 returned 18,000 / 17,852 which the agent read as "only 18k real rows in
a 5M-row table" and spun a long "sampled dataset" theory. Verified against raw SQL: those
numbers are exactly correct as distinct `item.sk` counts (item dim has 18,000 rows; 17,852
items have a null-quantity line). By-design semantics, not a bug; do not re-file. The
grainless HAVING co-grain and surrogate-key fan-out verdicts remain NOT-A-BUG, and the
`file list /` sandbox escape is already filed in `q14_file_list_unsandboxed_crash_bug.md`.
