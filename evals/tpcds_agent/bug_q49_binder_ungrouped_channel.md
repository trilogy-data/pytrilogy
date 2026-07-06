# q49 — BinderException: ORDER BY expression re-rendered from an ungrouped raw column

**Run:** `evals/tpcds_agent/results/20260706-135542_enriched` — q49 enriched leg, 553k tokens, FAILED.
**Classification:** FRAMEWORK bug. Trilogy emitted invalid SQL (a `group_to_grain` SELECT whose `ORDER BY` references a base column absent from `GROUP BY`). BinderException from generated SQL is always framework.

## Symptom

```
Unexpected error in query49.preql: (_duckdb.BinderException) Binder Error:
column "s_channel" must appear in the GROUP BY clause or must be part of an
aggregate function.
LINE 120:     LOWER("vacuous"."s_channel")  asc,
```

The failing final node (from the agent's query49.preql, which used `lower(s.channel) as channel`
and `order by lower(s.channel)`):

```sql
SELECT "vacuous"."channel", "vacuous"."item", "vacuous"."return_ratio",
       "vacuous"."return_rank", "vacuous"."currency_rank"
FROM "vacuous"
WHERE "vacuous"."return_rank" <= 10 or "vacuous"."currency_rank" <= 10
GROUP BY 1, 2, 3, 4, 5                       -- groups only the 5 declared outputs
ORDER BY LOWER("vacuous"."s_channel") asc,   -- re-derives raw s_channel: NOT in GROUP BY
         "vacuous"."return_rank" asc, ...
```

`vacuous` (a window CTE) exposes raw `s_channel`, but the outer node is `group_to_grain` and
its GROUP BY lists only output columns 1..5. `s_channel` is not among them → ungrouped reference.

## Minimal repro

Harness: `scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')` over the run workspace;
`eng.generate_sql(body)[-1]` then `eng.execute_raw_sql(sql).fetchall()`.

```
import raw.all_sales as s;
where s.date.year = 2001 and s.date.month_of_year = 12
select
    lower(s.channel) as channel,
    s.item.id as item,
    rank(s.item.id) over (partition by s.channel order by sum(s.quantity) asc) as r
order by lower(s.channel) asc
limit 100;
```
→ `Binder Error: column "s_channel" must appear in the GROUP BY clause`.

## Trigger matrix

| # | shape | result |
|---|-------|--------|
| C0 | workspace file as-is: `s.channel as channel` (identity) + `order by s.channel` + windows | OK — order item is a BuildConcept matching output `channel`; renders `ORDER BY vacuous.channel` |
| C1 | `lower(s.channel) as channel` + `order by lower(s.channel)` + windows + having | **ERROR** (matches log) |
| C2 | `lower(s.channel) as channel` + `order by channel` (alias) + windows + having | OK |
| C3 | C1 minus HAVING | **ERROR** — having/where-wrap NOT required |
| C4 | `lower(s.channel) as channel` + `order by lower(s.channel)`, **no window** | OK — final node IS the aggregate; GROUP BY 1 = `LOWER(s_channel)`, ORDER BY matches grouped expr |
| C5 | minimal: one window output + `order by lower(s.channel)` | **ERROR** (smallest) |
| C6 | C5 with `order by channel` (alias) | OK |
| C7 | C5 with `order by s.item.id` | OK |

**Minimal combination:** (a) a **window function** in the output (forces the extra window CTE +
a spurious `group_to_grain` wrapper node), AND (b) an `ORDER BY` on a **derived expression**
(`lower(s.channel)`) whose definition duplicates a select output but is kept as an anonymous
`BuildFunction`, not bound to that output concept. Remove the window (C4) or point ORDER BY at
the output alias (C2/C6) and it renders valid SQL.

## Root cause (file:line)

Introspection of the final CTE (`ProcessedQuery.ctes[-1]`) for the minimal repro:
- `group_to_grain=True`, `group_concepts = [r, channel, item]` (declared outputs only),
  `hidden_concepts=[]`, and the ORDER BY item's `expr` is a **`BuildFunction`** (`lower(s.channel)`),
  NOT a `BuildConcept`. For `order by channel` the item is a `BuildConcept` (→ works).

Two framework facts combine:

1. **`trilogy/dialect/base.py:980` `render_order_item`** — the alias-reference short-circuit
   (lines 988-998) only fires when `isinstance(order_item.expr, BuildConcept)` and its address is
   in `output_columns`. An ORDER BY on a raw expression (`BuildFunction`) skips it and hits line
   1000 `render_expr(order_item.expr, cte=cte)`, which re-renders `LOWER(<parent>.s_channel)`
   pulling `s_channel` from the parent window CTE via `source_map`.

2. **`trilogy/dialect/base.py:2148` `render_cte_group_by`** — for a `group_to_grain` CTE it emits
   GROUP BY built only from `cte.group_concepts` (the declared outputs, rendered as indices). It
   has no knowledge of the extra base columns the ORDER BY expression in (1) reaches for, so
   `s_channel` is never added to the group set.

Net: a `group_to_grain` node emits an ORDER BY expression over a non-grouped source column →
invalid SQL. Upstream, the real defect is that the ORDER BY expression `lower(s.channel)` is left
as an anonymous `BuildFunction` instead of being deduped/bound to the structurally identical
output concept `channel` (which *is* grouped). That binding/dedup would happen in the order-by
resolution path (`trilogy/parsing/v2/rules/order_rules.py` / `select_finalize.py` /
`query_processor.py`); once the ORDER BY points at the output concept, `render_order_item` takes
the alias path and the query is valid (cf. C2/C6).

Secondary observation: wrapping already-unique window outputs in a `group_to_grain` node that
groups by all outputs (including the `rank()` column `r`) is a redundant de-dup; it is only *harmful*
because of the ORDER BY re-render above. A non-grouping passthrough final node would also sidestep
the error.

## Agent impact

The agent's natural spelling — lowercase the channel label (`lower(s.channel) as channel`, required
by the task's 'web'/'catalog'/'store' output) and sort by it (`order by lower(s.channel)`) — is
exactly the trigger. The workaround (`order by channel` referencing the alias) is non-obvious, so
the agent burned 553k tokens thrashing on a valid-looking query. Not an agent error.

## FIXED 2026-07-06 (two layers)

1. **Parse-level binding (primary):** `_substitute_order_by_outputs` in
   `trilogy/parsing/v2/select_finalize.py` replaces the old aggregates-only ORDER BY substitution
   with a full structural-signature match (`_order_expr_signature`, composing the window /
   aggregate / scalar signatures). Any ORDER BY expression — scalar derivation (`lower(s.channel)`),
   window, aggregate, or a **compound of aggregates** (`grouping(a)+grouping(b)`, q80 case C) —
   that structurally equals a projected (possibly hidden) SELECT output is rebound to that output's
   alias before validation, so `render_order_item` takes the alias path. Fixes C1/C3/C5 and the
   q80 compound-`grouping()` auto-resolve miss; unprojected inline aggregates still raise the
   guided syntax error.

2. **Render-level backstop:** `render_order_item` / `_order_expr_needs_group_wrap` in
   `trilogy/dialect/base.py` — when a grouping `group_to_grain` node's ORDER BY re-renders a purely
   scalar expression over columns absent from its GROUP BY (e.g. `order by <raw source col>` where
   only a derivation of it is projected), the rendered expression is wrapped in `MIN(...)`: a no-op
   for group-determined inputs, deterministic min-member ordering otherwise. Never wraps
   aggregates/windows, and skips expressions textually identical to a grouped expression (C4).

Guards: `tests/engine/test_duckdb_orderby_derived_expr.py`. Both the minimal repro and the agent's
original q49 spelling verified working against the run workspace.
