# P0 (filed): WHERE-referenced pinned aggregate "silently fails to filter" (q16, 233 vs 178)

**Investigation verdict: NOT A BUG in the engine. The divergence is real and reproduces,
but it is the designed and documented dual-scope semantic, and the "wrong" number (233)
is byte-exact the official TPC-DS q16 reference. The truly wrong number is 178, which the
agent shipped after "fixing" a correct query. Details, evidence, and one genuine
incidental defect (a planner crash) below.**

- Filed as: explicit `auto x <- count_distinct(...) by k;` referenced in WHERE silently
  fails to filter, while the same auto in an inline `?` filter works; pin allegedly
  re-grained or dropped.
- Found: the pin is honored (per-order grain in generated SQL), the predicate is applied
  (inner join gate present), and both forms behave exactly per spec. They differ in
  *input population*, not grain: WHERE gates on the population value (aggregate computed
  over unfiltered base data), inline `?` sees WHERE-filtered inputs. On this data the
  population-scope predicate is vacuously true, which *looks like* a dropped filter.

## Symptom

Run `repeat_q16_20260815-164641_ingest`, rep r04 (`agent_log.q16.r04.conversation.txt`,
messages 32-36). Model: the run's `workspace/root/*.preql` + `workspace/warehouse.duckdb`
(TPC-DS sf=1), db copied to scratchpad before executing.

```text
import root.catalog_sales as cs;
import root.catalog_returns as cr;
auto order_wh_count <- count_distinct(cs.warehouse.warehouse_sk) by cs.order_number;
-- window = ship_date.date in ['2002-02-01','2002-04-02'], ship_addr.state='GA',
--          call_center.county='Williamson County'
```

| Form | Result |
| --- | --- |
| WHERE: `where <window> and order_wh_count > 1 and cs.order_number not in cr.order_number`, `select count(cs.order_number), sum(...)` | **233** / 1095837.99 / -143869.23 |
| Inline: `count(cs.order_number ? order_wh_count > 1 and cs.order_number not in cr.order_number)` with `<window>` in WHERE | **178** |

Both reproduced on the current tree via `evals/common/scoring.make_scoring_engine`
(`repro_q16_where.py` in the session scratchpad): `where_form -> 233`,
`inline_form -> 178`.

## Reproducible-on

`2435237d4` (branch `more-benchmarking`), engine = the tree's planner via
`evals/common/scoring.py:403 make_scoring_engine`, db = copy of
`evals/tpcds_agent/results/repeat_q16_20260815-164641_ingest/workspace/warehouse.duckdb`.

## Ground truth verification

Raw DuckDB against the same db:

1. **Official TPC-DS q16** (correlated `exists` over the FULL `catalog_sales`, 60-day
   window from 2002-02-01, GA, Williamson County, `not exists` returns):
   **(233, 1095837.99, -143869.23)**. Identical to the WHERE form and to the agent's
   first (rowset-membership) answer that it later discarded.
2. **Population-scope decomposition**: per-order `count(distinct cs_warehouse_sk)` over
   the FULL table, then window: all **607** windowed orders have count > 1, so
   `window AND multi_wh(population) AND no_returns` = **233**. The WHERE predicate was
   applied and was *vacuously true*, not dropped.
3. **Filtered-scope decomposition**: per-order distinct-warehouse count over only the
   windowed rows: 475 of 607 orders multi-wh, intersect no-returns = **178**. Exactly the
   inline form.

So 233 = population scope (and the TPC-DS reference), 178 = WHERE-filtered scope. Each
form computes its documented scope correctly.

## Minimal repro

Fully local, single datasource, no imports, no FK namespaces, no membership predicate
(`synthetic_scope2.py` in the session scratchpad):

```text
key line int;
property line.order_id int;
property line.wh int;
property line.region string;
datasource lines (line: line, order_id: order_id, wh: wh, region: region)
grain (line)
query ''' select 1 as line, 10 as order_id, 1 as wh, 'A' as region
union all select 2, 10, 2, 'B'
union all select 3, 20, 1, 'A'
union all select 4, 20, 2, 'A'
union all select 5, 30, 1, 'A' ''';

auto owc <- count_distinct(wh) by order_id;

select count_distinct(order_id) as n where region = 'A' and owc > 1;   -- 2 (population owc)
select count_distinct(order_id ? owc > 1) as n where region = 'A';     -- 1 (filtered owc)
```

Order 10 has warehouses {1,2} globally but only {1} in region A. Sharpest cell:

```text
select order_id, owc where region = 'A' and owc > 1 order by order_id asc;
-- [(10, 1), (20, 2)]
```

A row passes `where owc > 1` while *projecting* `owc = 1`: the WHERE gates on the
population twin (value 2), the select output recomputes over the filtered rows (value 1).
That split is the explicitly stated contract of the dual-scope normalization (see Root
cause).

**Incidental genuine defect found while minimizing** (same model):
`select order_id, count(line ? owc > 1) as n where region = 'A';` crashes with
`UnresolvableQueryException: Query planning produced a circular dependency between group
nodes` (`grp:aggregate:d0:local.order_id:... <-> grp:filter:d*:local.order_id:...`),
raised at `trilogy/core/processing/v4_helper/strategy_builder.py:2039`; the working
variants also print `[v4] group-graph lineage cycle, skipping concept-set pass` warnings.
This is a hard error, not a silent wrong result; worth its own ticket.

## Trigger matrix

Starting hypothesis was structural; every structural candidate is eliminated. The trigger
is *data-shaped*: a plain WHERE predicate that changes the per-key aggregate value
between population and filtered scope.

| Cell | Result |
| --- | --- |
| (evidence, prior) User's local synthetic: `auto owc <- count_distinct(wh) by order_id`, no co-present row predicate that alters per-order wh sets | No divergence (negative cell, confirmed expected: with no scope-changing peer predicate, population and filtered values coincide, so both forms agree) |
| (a) Imported dim key through nullable FK namespace | **Not required**: local `property line.wh` reproduces |
| (b) Co-present membership `not in cr.order_number` | **Not required**: absent from minimal repro |
| (c) Co-present plain filters on other joined namespaces | **Required in spirit**: any plain WHERE predicate that shrinks some key's group (here `region='A'`); this is the actual trigger |
| (d) count over KEY vs property | **Not required**: property `wh` reproduces |
| (e) `by` key being a key of the imported model | **Not required**: local property `order_id` reproduces |
| Control: `where owc > 1` alone (no plain predicate) | 2 = both scopes agree, no divergence |
| Real query, documented remedy `where <window> then where order_wh_count > 1 and cs.order_number not in cr.order_number` | **178**: staged WHERE computes the aggregate over rows passing earlier stages, exactly as documented (`trilogy/ai/constants.py:86`, `docs/staged_where_design.md`) |
| Rowset cell from the log (`select cs.order_number ... where <window> and order_wh_count > 1` = 233 despite order_number select grain) | Same mechanism; grain was never the issue, WHERE inside a rowset also gates at population scope |

## Generated-SQL analysis

WHERE form (`q16_where_form.sql`, CTE names from the run): the pinned aggregate is
neither dropped nor re-grained.

```sql
uneven as (            -- aggregate INPUT: full catalog_sales (population scope);
  SELECT cs_order_number, cs_warehouse_sk FROM catalog_sales
  WHERE not exists (... catalog_returns ...) GROUP BY 1, 2),
yummy as (             -- pin honored: per-order grain
  SELECT cs_order_number FROM uneven GROUP BY 1
  HAVING count(distinct cs_warehouse_warehouse_sk) > 1),
abundant as (          -- window predicates, order grain
  SELECT cs_order_number FROM ... date_dim/customer_address/call_center joins
  WHERE d_date BETWEEN ... and ca_state = 'GA' and cc_county = ...),
vacuous as (           -- the predicate IS applied, as an inner-join gate
  SELECT cs_order_number FROM abundant INNER JOIN yummy USING-equivalent)
SELECT count(...) FROM vacuous
```

The `> 1` condition lands in `yummy`'s HAVING at exactly the pinned grain and gates the
final rows via `vacuous`. The window predicates are not pushed into `uneven`, so the
counts are population counts, all > 1 on this data, hence 233. (The `not exists` returns
predicate *is* pushed into `uneven`; being an order-level predicate it cannot change a
per-order distinct-warehouse count, so it is harmless here, but it is a visible
asymmetry in condition placement.)

Inline form (`q16_inline_form.sql`): the window predicates are applied in `abundant`
BEFORE the aggregate CTE `yummy` computes `count(distinct ...)`, then a
`CASE WHEN order_wh_count > 1 and not exists(...)` feeds the count. Filtered scope, 178.

## Root cause (file:line)

There is no code path that re-grains or drops a pinned `by`. The divergence is the
designed dual-scope semantic, implemented and documented at:

- `trilogy/core/where_scope_normalization.py:1-20` (module contract): "the WHERE gates
  rows using the POPULATION value at the computation's own grain (ignoring its peers in
  the clause), while the select output recomputes over the WHERE-filtered rows."
- `trilogy/core/where_scope_normalization.py:481` `normalize_select_where_scope`: mints
  the population-scope twin (`*_wscope`) for any WHERE reference to a cross-row concept;
  the twin is what produced `(10, 1)` passing `owc > 1` in the minimal repro.
- Invoked from `trilogy/core/models/build.py:4252` in `Factory._build_select_lineage`.
- Inline/selected scope (the 178 side): WHERE filters feed selected aggregates' inputs,
  including aggregates referenced inside an inline `?`; documented at
  `trilogy/ai/constants.py:86` ("Aggregates/windows in WHERE do not filter each other's
  inputs. Use inline filters, or a staged `then where` chain: `where x = 5 then where
  sum(y) > 10` is `where x = 5 and sum(y ? x = 5) > 10`").
- Grain doc the filing cited: `trilogy/ai/syntax_examples.py:900-903` documents grain
  inheritance and the `by` pin remedy. The pin governs *grain* and it worked. The
  "(like `having`)" phrase there is about grain defaulting only; it reads as if WHERE
  aggregates also see filtered rows like HAVING does, which is the misleading half-truth
  that sent the agent (and this filing) down the wrong path. Scope is documented in
  `constants.py:86`, not here.
- Prior art: the 2026-07-16 verdict already classified "WHERE row predicates do not leak
  into WHERE-aggregate inputs" as intended; `trilogy/core/scope_diagnostics.py:1078`
  even ships a `where_aggregate_inherited_grain` diagnostic, and
  `docs/SPEC_query_derived_value_scopes.md` exists precisely to make this scope split
  observable (`run --scope`).

## Secondary (having-rowset membership)

Reproduced on the current tree: `rowset multi_wh <- select cs.order_number having
count_distinct(cs.warehouse.warehouse_sk) > 1;` then
`count(cs.order_number ? cs.order_number in multi_wh.order_number)` under the window
returns **607**. Not a bug either: the rowset has no WHERE, so its HAVING computes over
the full table, where ALL 607 windowed orders are multi-warehouse (raw-SQL verified:
population multi-wh = 607 of 607). Membership was never "always-true"; the set genuinely
contains every windowed order. The agent's 475 cross-check used window-scoped counts, a
different (and for TPC-DS q16, wrong) population.

## Verdict

- **Engine: NOT A BUG.** Pin honored, predicate applied, both forms match their
  documented scopes, and the WHERE form equals the official TPC-DS q16 answer
  (233 / 1095837.99 / -143869.23). The agent regressed a correct answer to 178.
- **Real follow-ups:**
  1. Planner crash (`UnresolvableQueryException`, strategy_builder.py:2039) on
     `select k, count(x ? pinned_agg_by_k > cmp) where <plain>`; file separately.
  2. Doc sharpening: `syntax_examples.py:900` "(like `having`)" should state that the
     analogy covers grain only, and point at `then where` / inline `?` for
     filtered-scope gating; this exact misreading produced a shipped wrong answer.
  3. Agent-guidance gap: the `run --scope` report (SPEC_query_derived_value_scopes.md)
     exists to disambiguate exactly this; the eval agent never consulted it.
