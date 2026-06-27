# QA: q80 union-arm SQL puts the returns LEFT JOIN before the selective filters → ~160s/arm

**Status:** FIXED (2026-06-26) — render-time join reorder pulls selective INNER joins ahead of
optional LEFT OUTER joins. q80 now executes in **1.1s** (was 258–262s / 180s scoring timeout).
See "QA conclusion" and "Fix" below.
**Surfaced by:** TPC-DS q80 (run `20260626-212144`) — **scoring timed out at 180s**; the agent's own
runs took 258s and 262s. Planner is fast (`generate_sql` = 0.3s); the cost is pure **execution** of
the generated SQL.
**Driver note:** this is a **wall-time / scoring-timeout (pass/fail)** issue, NOT a token driver —
the agent's q80 tokens came from normal iterations, not the slowness. File under correctness/perf.

## Source Trilogy query (`query80.preql`)

This is the agent's actual query — a 3-arm `union(...)` (store/catalog/web), each arm filtering its
fact by the same predicates and LEFT JOINing its outlet dimension, then an outer ROLLUP. **Note: the
`return_amount` / `return_net_loss` referenced in each arm are what pull in the per-channel returns
fact via a LEFT join — that is the join the generated SQL places before the filters.**

```trilogy
import raw.all_sales as s;
import raw.store as store;
import raw.catalog_page as catalog_page;
import raw.web_site as web_site;

with combined as union(
  # Store channel arm
  (where s.channel = 'STORE'
    and s.date.date between '2000-08-23'::date and '2000-09-22'::date
    and s.item.current_price > 50
    and s.promotion.channel_tv = 'N'
    and s.outlet_id is not null
   select
      'store channel' as channel_label,
      concat('store', store.text_id) as outlet_identifier,
      s.ext_sales_price as sales_amt,
      coalesce(s.return_amount, 0) as return_amt,
      s.net_profit as profit_amt,
      coalesce(s.return_net_loss, 0) as return_loss_amt
    left join s.outlet_id = store.id
  ),
  # Catalog channel arm
  (where s.channel = 'CATALOG'
    and s.date.date between '2000-08-23'::date and '2000-09-22'::date
    and s.item.current_price > 50
    and s.promotion.channel_tv = 'N'
    and s.outlet_id is not null
   select
      'catalog channel' as channel_label,
      concat('catalog_page', catalog_page.text_id) as outlet_identifier,
      s.ext_sales_price as sales_amt,
      coalesce(s.return_amount, 0) as return_amt,
      s.net_profit as profit_amt,
      coalesce(s.return_net_loss, 0) as return_loss_amt
    left join s.outlet_id = catalog_page.id
  ),
  # Web channel arm
  (where s.channel = 'WEB'
    and s.date.date between '2000-08-23'::date and '2000-09-22'::date
    and s.item.current_price > 50
    and s.promotion.channel_tv = 'N'
    and s.outlet_id is not null
   select
      'web channel' as channel_label,
      concat('web_site', web_site.text_id) as outlet_identifier,
      s.ext_sales_price as sales_amt,
      coalesce(s.return_amount, 0) as return_amt,
      s.net_profit as profit_amt,
      coalesce(s.return_net_loss, 0) as return_loss_amt
    left join s.outlet_id = web_site.id
  )
) -> (channel_label, outlet_identifier, sales_amt, return_amt, profit_amt, return_loss_amt);

select
    combined.channel_label,
    combined.outlet_identifier,
    sum(combined.sales_amt) by rollup combined.channel_label, combined.outlet_identifier as sales,
    sum(combined.return_amt) by rollup combined.channel_label, combined.outlet_identifier as returns,
    sum(combined.profit_amt) by rollup combined.channel_label, combined.outlet_identifier
      - sum(combined.return_loss_amt) by rollup combined.channel_label, combined.outlet_identifier as profit
order by combined.channel_label nulls first, combined.outlet_identifier nulls first
limit 100;
```

## Finding (experimentally measured, not estimated)

Each `union(...)` arm filters a sales fact (date window + item price>50 + promo channel_tv='N') and
LEFT JOINs its returns table. In the generated SQL the **LEFT OUTER JOIN to the returns table is
emitted BEFORE the selective INNER-join filters**, e.g. the store arm (`concerned` CTE):

```sql
FROM vacuous                          -- full store_sales, 2,750,943 rows
LEFT OUTER JOIN store_returns ON ... item AND ticket
INNER JOIN thoughtful  (date window) ON date_id      -- 800x selective, applied AFTER the LEFT JOIN
INNER JOIN questionable(promo)       ON promo_id
INNER JOIN cooperative (item price)  ON item_id
WHERE ...
```

Because the outer join sits below the inner filters (and LEFT joins constrain reordering), DuckDB
materializes the ~2.75M-row outer join first, then discards ~99.9% of it.

**Measured cardinality/time (store arm):**
- `vacuous` (raw scan): **2,750,943 rows**
- `concerned` (arm after filters): **3,453 rows — in 159.9s**

Three such arms → the ~260s total.

## Proof an equivalent efficient plan exists

Filtering the fact FIRST, then LEFT JOINing returns — semantically identical (the filters are all on
the SALE, not the return) — produces the **same 3,453 rows in 0.0s**:

```sql
WITH filtered AS (
  SELECT ss.SS_ITEM_SK, ss.SS_TICKET_NUMBER, ss.SS_STORE_SK, ss.SS_EXT_SALES_PRICE, ss.SS_NET_PROFIT
  FROM store_sales ss
  JOIN date_dim d  ON ss.SS_SOLD_DATE_SK=d.D_DATE_SK
       AND cast(d.D_DATE as date) BETWEEN date '2000-08-23' AND date '2000-09-22'
  JOIN promotion p ON ss.SS_PROMO_SK=p.P_PROMO_SK AND p.P_CHANNEL_TV='N'
  JOIN item i      ON ss.SS_ITEM_SK=i.I_ITEM_SK   AND i.I_CURRENT_PRICE>50
  WHERE ss.SS_STORE_SK is not null
)
SELECT count(*) FROM filtered f
LEFT JOIN store_returns sr ON f.SS_ITEM_SK=sr.SR_ITEM_SK AND f.SS_TICKET_NUMBER=sr.SR_TICKET_NUMBER;
-- 3,453 rows, 0.0s
```

160s → 0.0s for identical output. So the slowness is the **join order the framework emits**, not the
query the agent wrote.

## QA questions for the investigator

1. Confirm the LEFT-OUTER-JOIN-before-selective-INNER-filters ordering is what the planner emits for a
   `union(...)` arm that filters a fact and LEFT JOINs another fact (returns). Is this general to
   "anchor fact + optional joined fact + selective dim filters", or specific to the union/TVF path?
2. Should the planner order the selective (INNER, dim-filter) joins ahead of the OPTIONAL (LEFT)
   join, or push the sale-side filters into the fact scan before the outer join? Both are
   semantically safe here because every filter is on the anchor fact.
3. Is the agent's query reasonable/idiomatic (it is — union arms each filter their fact and attach
   returns), confirming the fix belongs in generation, not guidance?

## Repro

Save the Trilogy above as `query80.preql`, then:

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-212144/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(open('query80.preql').read())[-1]   # fast: ~0.3s
eng.execute_raw_sql(sql)   # ~260s — scoring's 180s cap → timeout/fail
```

Inspect the generated `sql`: each arm's CTE (`concerned`/`abundant`/`scrawny`) shows the
`LEFT OUTER JOIN <returns>` listed before the three `INNER JOIN`s to the date/promo/item filter CTEs.

## QA conclusion (2026-06-26) — CONFIRMED, fix belongs in generation

**1. Ordering reproduced from the real generated SQL.** Ran `generate_sql` on the agent's exact
`query80.preql`. The store arm CTE `concerned` is literally:

```sql
FROM "vacuous"                                 -- full store_sales scan
LEFT OUTER JOIN "store_returns" on vacuous.s_item_id=sr.SR_ITEM_SK AND vacuous.s_order_id=sr.SR_TICKET_NUMBER
INNER JOIN "thoughtful"    on vacuous.s_date_id = thoughtful.s_date_id     -- date window (selective)
INNER JOIN "questionable"  on vacuous.s_promotion_id = questionable.s_promotion_id
INNER JOIN "cooperative"   on vacuous.s_item_id = cooperative.s_item_id    -- item price>50
```

Every join's ON references **only** the anchor `vacuous` + its own right table — a pure star, no
join depends on another. So the LEFT can move after the INNERs with no correctness risk here.

**2. Isolated the join order as the sole cause (controlled A/B on the q80 DB).** Same four CTEs,
identical predicates, only the FROM-clause order changed:

| FROM-clause order                         | rows  | time      |
|-------------------------------------------|-------|-----------|
| LEFT(returns) first, then 3 INNER filters | 3,453 | **152.93s** |
| 3 INNER filters first, then LEFT(returns) | 3,453 | **0.54s**   |

DuckDB does **not** reorder the selective INNER joins below the outer join on its own — it
materializes the full ~2.75M-row `store_sales ⟕ store_returns` first, then discards 99.9%. Same
result set, ~280x. Three arms → the ~260s total / 180s scoring timeout.

**Answers to the QA questions:**
1. The ordering is what the planner emits, and it is **general**, not union-specific. Join order
   comes from `resolve_join_order_v2` (`join_resolution.py`), which orders by pivot / partial /
   nullable / grain-size only — it is **selectivity- and INNER-vs-OUTER-order-blind**.
   `ensure_content_preservation` only adjusts join *types*, never order; the dialect render
   (`base.py`, `joins=[render_join(j) ... for j in final_joins]`) iterates `cte.joins` verbatim.
   So any "anchor fact + optional LEFT-joined fact + selective INNER dim filters" shape hits this.
2. Yes — emit the selective INNER (dim-filter) joins **ahead of** the OPTIONAL LEFT/FULL joins.
   Reducing inner joins first is a near-universal win and never changes results, so it's the
   simplest safe lever. (Pushing the anchor-side filters into the fact scan before the outer join
   is an alternative but bigger change.)
3. The agent's query is idiomatic (each union arm filters its fact and attaches returns). The fix
   belongs in **generation**, not agent guidance.

**Proposed fix + risk.** Order joins so INNER joins precede OUTER (LEFT/RIGHT/FULL) joins, made
**dependency-aware**: a join may only be pulled earlier if its ON references only sources already
emitted before it (and an outer join may only be deferred past joins that don't reference its right
table). For the common star-off-anchor shape (q80, and TPC-DS return-fact arms generally) this is
unconditional. Cleanest seam is right where `resolve_join_order_v2` builds its `output` list (it
already has the connection graph to check dependencies), or a stable reorder pass over `cte.joins`
keyed on each join's referenced-source set. Low correctness risk **if** the dependency guard is
respected; a naive unconditional sort is unsafe when an INNER join's ON references an
outer-joined table (e.g. `A LEFT JOIN B` then `B INNER JOIN C`).

## Fix (implemented 2026-06-26)

`reorder_inner_before_left` in `trilogy/dialect/common.py`, called at the render boundary in
`base.py` (`final_joins = reorder_inner_before_left(cte.joins or [], cte.base_name)`). Chose the
render boundary because arm CTEs assemble their join list by concatenation (`self.joins +
other.joins`), not via `resolve_join_order_v2`, so that was the one chokepoint covering every path.

**Correctness model (why a render-time reorder is safe without re-running content preservation):**
- An INNER join is only bubbled ahead of **LEFT OUTER** joins, and only when none of the sources
  its ON clause reads are produced by a deferred LEFT join (dependency guard). A LEFT join preserves
  every left row and only adds nullable right columns, so an INNER filter on non-LEFT columns
  commutes with it unconditionally.
- **FULL and RIGHT OUTER joins are hard barriers** — they null-extend the anchor, so a later INNER
  filter on the anchor legitimately drops rows a reorder would resurrect. Nothing crosses them.
  Unnest / non-`Join` entries are barriers too. Relative order is otherwise preserved.

**Validation:** q80 1.1s / 29 rows (per-arm A/B earlier proved 152.93s→0.54s, identical 3,453
rows). Full suite `pytest -m "not adventureworks_execution"`: 4452 passed, the 5 failures are all
pre-existing (verified identical with the reorder disabled). Unit tests:
`tests/test_join_order_inner_before_left.py` (INNER-before-LEFT, FULL/RIGHT barriers,
dependency-respect, no-op cases).
