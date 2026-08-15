# P0: q95 filtered-count over its own grain key compiles to nested aggregates (`count(max(...))`)

## Symptom

The agent-written candidate
`evals/tpcds_agent/results/20260813-125008_enriched/workspace/query95.preql` contains

```
auto eligible_order <- (count_distinct(ws.warehouse.sk) by ws.order_number) > 1
                   and (count(ws.order_number ? ws.is_returned) by ws.order_number) > 0;
```

The second conjunct, a count of a filter-virtual of the grain key at that same
grain, compiles to a CTE containing a nested aggregate:

```sql
questionable as (
SELECT
    coalesce("ws_web_returns"."WR_ORDER_NUMBER","ws_web_sales"."WS_ORDER_NUMBER") as "ws_order_number",
    count(max(CASE WHEN ("ws_web_returns"."WR_ORDER_NUMBER" is not null) = True
              THEN coalesce("ws_web_returns"."WR_ORDER_NUMBER","ws_web_sales"."WS_ORDER_NUMBER")
              ELSE NULL END)) as "_virt_agg_count_3324386680180467_wscope"
...
GROUP BY 1)
```

DuckDB rejects it on execute:

```
Binder Error: aggregate function calls cannot be nested
LINE 23:     count(max(CASE WHEN ("ws_web_returns"."WR_ORDER_NUMBER" is not...
```

Note this is not only invalid SQL: even on an engine that allowed the nesting,
`count(max(...))` per group is not the value the construct denotes, so a
"quote it differently" renderer patch is not sufficient on its own.

## Reproducible-on: 2435237d4

Regenerated via the scoring harness on this tree
(`scoring.make_scoring_engine(db, workspace, 'tpcds')` then
`eng.generate_sql(body)`) against a scratch copy of the run's
`warehouse.duckdb`; generation emits the nested aggregate and read-only
execution reproduces the Binder error verbatim.

## Minimal repro

Fully standalone, no eval workspace or database needed (generation only):

```
key order_number int;
key item int;
property <order_number, item>.flag bool;

datasource lines (
  o: order_number,
  i: item,
  f: flag
)
grain (order_number, item)
address lines_tbl;

select
  order_number,
  count(order_number ? flag) by order_number as flagged
limit 5;
```

`Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(...)`
produces:

```sql
SELECT
    "quizzical"."order_number" as "order_number",
    count(max(CASE WHEN "quizzical"."flag" = True THEN "quizzical"."order_number" ELSE NULL END)) as "flagged"
FROM "quizzical"
GROUP BY 1
```

Essential ingredients: an aggregate whose argument is a filter-virtual
(`x ? cond`) where (a) the filtered content's keys are covered by the
aggregate's `by` grain (here: the grain key itself), (b) the filter predicate
reads a column outside that grain (here: line-level `flag`), and (c) the
aggregate is computed in the same group-to-grain CTE.

## Trigger matrix

All run against the q95 workspace model (`import raw.web_sales as ws`) on
2435237d4; "clean" = generated SQL has no nested aggregate and executes.

| # | Variant | Result |
|---|---------|--------|
| A | Full candidate query95.preql | NESTED, Binder error |
| B | `count(ws.order_number ? ws.is_returned) by ws.order_number` (minimal, workspace) | NESTED, Binder error |
| C | Standalone synthetic model above (no workspace) | NESTED |
| D | Predicate inside grain: `count(ws.order_number ? ws.order_number > 0) by ws.order_number` | clean (`count(CASE WHEN ...)`), executes |
| E | Content not a grain key: `count(ws.item.sk ? ws.is_returned) by ws.order_number` | clean |
| F | No outer aggregate: project `ws.order_number ? ws.is_returned` at grain | clean (the intended q16 `max(CASE...)` collapse, standalone) |
| G | Rowset staging: `with returned_lines as select ws.order_number, ws.item.sk where ws.is_returned;` then count over the rowset | clean |
| H | Rewrite as `sum(cast(ws.is_returned as int)) by ws.order_number` | clean |
| J | `count_distinct(ws.order_number ? ws.is_returned) by ws.order_number` | NESTED (`count(distinct max(CASE ...))`) |
| K | Same filtered count at top-level grain (no `by`, no regroup CTE) | clean |

So: the failure is specific to (filtered content keys covered by grain) AND
(predicate outside grain) AND (aggregate computed in the grouping CTE itself).
Staging through a rowset, moving the predicate inside the grain, counting a
non-grain concept, or dropping the `by` regroup all avoid it.

## Root cause file:line

The q16 count(key) double-count fix added a renderer-level collapse: a
filter-virtual whose keys are covered by a grouping CTE's grain but whose
predicate reads outside it is excluded from GROUP BY
(`trilogy/core/models/execute.py:683-684`, gate predicate
`CTE.filter_collapses_to_grain` at `trilogy/core/models/execute.py:559-590`)
and its rendered `CASE WHEN ...` is wrapped in `MAX(...)` to collapse the
per-row `{content, NULL}` fan-out:

- `trilogy/dialect/base.py:1371-1379` - the `FUNCTION_MAP[FunctionType.MAX]`
  wrap in `render_concept_sql`. This hook fires unconditionally on every render
  of the concept in that CTE; it has no awareness of whether the render call is
  a projected SELECT column (the intended q16 case, variant F) or the argument
  of an aggregate being composed in the same CTE.
- The offending path: `_render_concept_sql` renders the local count metric via
  its `AGGREGATE_ITEMS` branch (`trilogy/dialect/base.py:1507-1513`), rendering
  each function argument with `render_expr` (base.py:1508-1511); the
  filter-virtual argument hits `render_expr`'s `BuildConcept` branch
  (`trilogy/dialect/base.py:2513-2519`), which calls the public
  `render_concept_sql`, which applies the MAX wrap, and the aggregate operator
  is then applied around it at base.py:1513 -> `count(max(CASE ...))`.

Introduced by commit a9c9e3bc6 "[Bug]: Fuzz Coverage (#637)" (sole
`git log -S filter_collapses_to_grain` hit under `trilogy/`).

There is a planner dimension too: for Trilogy's deduplicate-to-grain count
semantics, the collapse (dedup of the filtered key to the grain) and the count
over the collapsed value are two grain levels, but the plan materializes both
in one CTE (`questionable` computes the join, the grain dedup, and the count
together), which is what forces the renderer to nest.

## Verdict

Framework bug, P0 class per the standing rule: generated SQL must never draw a
Binder error from the database.

The right contract here is correct-compile, not clear-reject:

- The construct is well-formed, expressible Trilogy with well-defined
  semantics: `count(key ? cond) by key` is the documented existence-count
  idiom (the workspace model itself advertises
  `sale_line_item_counter ? is_returned` counting), and the engine already
  compiles the identical construct correctly in adjacent configurations
  (variants D, E, K) - there is no principled semantic line an authored-error
  could sit on.
- Correct output exists and is simple: either stage the collapse one CTE
  earlier (inner group-to-grain CTE emitting `max(CASE ...)` as a plain
  column, outer CTE counting it - exactly the two-CTE shape the engine
  already builds for variant F plus a downstream consumer), or, renderer-side,
  since `filter_collapses_to_grain` requires the content's keys to be covered
  by the grain, the content value is grain-determined and
  `count(distinct CASE WHEN cond THEN content END)` computes the same
  collapsed count in one level. The minimal invariant either way: the MAX
  collapse hook must not fire when the render is an argument position of an
  aggregate computed in the same CTE.

Repro artifacts (scratchpad, session-local): `repro_q95.py`,
`minimize_q95.py`, `q95_generated.sql`, `standalone_generated.sql`.
