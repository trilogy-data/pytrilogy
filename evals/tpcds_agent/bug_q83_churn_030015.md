# q83 silent churn — aggregate condition in WHERE is evaluated at GLOBAL grain (no-op filter)

Run: `evals/tpcds_agent/results/20260629-030015_enriched` — q83 burned ~1.24M tokens,
FAILED, with **zero** exit_code:1 errors. Pure silent thrash: every run returned
exit 0 / 100 rows, and the agent submitted believing it was correct.

## The obstacle (silent wrong rows)

q83 = three-channel (store/catalog/web) return quantities for items that have at
least one return row in **each** of the three channels, restricted to the weeks
containing 2000-06-30 / -09-27 / -11-17.

The agent expressed the three-channel membership requirement as a top-level
**WHERE** over three derived booleans (`workspace/query83.preql:48-49`):

```
auto store_has_return   <- count(s.order_id ? s.channel='STORE'   and s.return_date.week_seq in (5244,5257,5264)) > 0;
auto catalog_has_return <- count(s.order_id ? s.channel='CATALOG' and ...) > 0;
auto web_has_return     <- count(s.order_id ? s.channel='WEB'     and ...) > 0;
where store_has_return and catalog_has_return and web_has_return
select s.item.text_id as item_code, store_qty as store_return_qty, ...
```

This is the natural reading of the spec. It silently produces the **wrong row set**:
the agent's final query returns **9000 items** (`full_row_count: 9000` in its own last
run, message 71), truncated to 100 — versus the correct **24**. Most output items have
returns in only ONE channel (e.g. `AAAAAAAAAAABAAAA` = store null, catalog null,
web 13), directly violating the three-channel constraint the WHERE was supposed to
enforce. The agent never noticed (always exit 0, always 100 rows) and spent its whole
budget tweaking unrelated null-quantity CASE logic.

## Minimal repro (wrong-vs-expected)

Harness: `make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')` on the run workspace.
Same three `count(...) > 0` booleans, only the clause they sit in changes:

| Variant | placement of the three `count(...)>0` booleans | rows | single-channel garbage rows |
|---|---|---|---|
| A (agent's form) | bare aggregate, in **WHERE** | **100** (full 9000) | **69 of 100** |
| B | `... by s.item.text_id > 0`, in **WHERE** | **24** | 0 |
| C | bare aggregate, in **HAVING** | **24** | 0 |

Canonical `tests/modeling/tpc_ds_duckdb/query83.preql` (adapted to `raw.*` imports)
builds and runs correctly on the current engine → **24 rows** with real per-channel
quantities (e.g. `AAAAAAAAAHFBAAAA` = 75 / 69 / 1). It uses `count(...) by item > 0`
in **HAVING** — i.e. exactly variants B/C. Expected = 24; agent shipped 9000.

## Root cause

`store_has_return` is `Derivation.BASIC` (top op is `>`) wrapping a bare aggregate
`count(...)` whose grain defaults to `Grain<Abstract>` (global, `by *`). When such a
concept is consumed **only by the WHERE clause**, the planner sources it at its own
declared grain. The generated SQL makes this explicit — the `yummy` CTE computes the
booleans with **no GROUP BY and no item key**:

```sql
yummy as (
  SELECT (count(...store...) > 0)   as store_has_return,
         (count(...web...)   > 0)   as web_has_return,
         (count(...catalog...) > 0) as catalog_has_return
  FROM uneven FULL JOIN abundant ON uneven.s_order_id = abundant.s_order_id)   -- no GROUP BY!
...
young as (... FULL JOIN yummy ON 1=1 WHERE yummy.store_has_return and yummy.catalog_has_return and yummy.web_has_return GROUP BY item)
```

Each boolean is a single **global scalar**, trivially TRUE over the whole returns
table, so `WHERE TRUE and TRUE and TRUE` is a no-op and every item survives.

The asymmetry is the defect: the SAME bare aggregate, when used as a SELECT output or
in HAVING, is correctly regrouped to the select grain (item) — see `store_qty`
(`Derivation.AGGREGATE`) which the `young` CTE groups by item, and variant C/B which
emit a `GROUP BY i_item_id` for the has_return CTE. Only the WHERE path sources the
aggregate at its abstract grain instead of co-grouping it with the select grain (item).

WHERE conditions are threaded through the planner as `context.conditions` and applied
as `preexisting_conditions` at a sourced node
(`trilogy/core/processing/concept_strategies_v3.py:240-453`; the condition's
`row_arguments` — here the abstract-grain aggregate concept — drive its sourcing
grain). For an aggregate-bearing WHERE predicate the framework should either co-grain
it to the select grain (as HAVING does) or reject "aggregate in WHERE without explicit
`by`"; instead it silently builds a global-grain filter.

## Classification: FRAMEWORK BUG (silent)

- Not question-spec: the canonical query expresses the identical intent and is correct.
- Not purely agent error: the agent's construct is a reasonable reading; the framework
  accepted it (exit 0) and silently mis-grained it. A clean error ("aggregate in WHERE
  needs explicit grain / use HAVING") or correct co-graining would have saved ~1.24M
  tokens. The agent had no signal — its own final run even reported `full_row_count:
  9000` while it asserted "keeps only items with a return in each channel."

Suggested fix direction (do NOT apply here): when a WHERE predicate's `row_arguments`
contain an aggregate/abstract-grain concept, either co-group it to the statement's
select grain before applying the filter, or raise an InvalidSyntaxException pointing
the author to HAVING / an explicit `by`.

Repro scripts used: ad-hoc (`/tmp/repro3.py`, `/tmp/repro5.py`) against the run
workspace; not committed.
