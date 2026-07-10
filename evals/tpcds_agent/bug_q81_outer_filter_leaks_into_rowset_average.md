# q81: outer rowset filter leaks into a partitioned average

## Classification

Framework bug: silent wrong-result SQL generation.

An output-only filter on one field of a rowset is incorrectly injected into a
separate `by`-partitioned aggregate over that rowset. In q81, filtering the
reported customers to current home state `GA` also restricts the population
used to compute the average return total for each returning-address state.

The agent deliberately created the aggregate rowset before the GA filter, so
this is not authored filter scoping.

## Impact on enriched q81

The candidate returned the same leading rows and values as the reference, but
the qualifying population diverged around the threshold:

| Average population | Qualifying GA customer/state rows |
|---|---:|
| All customers in each returning-address state (reference) | 367 |
| GA-home customers only (generated behavior) | 382 |

The candidate's full result contained exactly 382 rows. Both candidate and
reference were limited to 100, so the scorer reported 100 versus 100 with
different result sets.

## Original q81 shape

```preql
import catalog_returns as cr;

with totals as
where
    cr.date.year = 2000
    and cr.return_address.state is not null
select
    cr.billing_customer.id,
    cr.billing_customer.address.state as home_state,
    cr.return_address.state as return_state,
    sum(cr.return_amt_inc_tax) as customer_total;

auto state_avg <- avg(totals.customer_total) by totals.return_state;

select
    totals.id,
    totals.customer_total
having
    totals.home_state = 'GA'
    and totals.customer_total > 1.2 * state_avg;
```

The model at the time called the returning customer `billing_customer`; its
`explore` description explicitly identified that alias as the returning
customer. The role choice is unrelated to this bug.

## Minimal current-model reproduction

Catalog returns are now exposed through `catalog_sales`, so the equivalent
current-model query is:

```preql
import catalog_sales as cr;

with totals as
where
    cr.return_date.year = 2000
    and cr.return_address.state is not null
select
    cr.return_customer.id,
    cr.return_customer.address.state as home_state,
    cr.return_address.state as return_state,
    sum(cr.return_amount_inc_tax) as customer_total;

auto state_avg <- avg(totals.customer_total) by totals.return_state;

select
    totals.id,
    totals.customer_total
having
    totals.home_state = 'GA'
    and totals.customer_total > 1.2 * state_avg;
```

Generate SQL against the SF1 q81 database using the standard scoring engine:

```python
import sys
from pathlib import Path

sys.path.insert(0, "evals")
from common import scoring

model = Path("tests/modeling/tpc_ds_duckdb").resolve()
db = Path(
    "evals/tpcds_agent/results/20260709-105517_enriched/"
    "workspace/tpcds.duckdb"
).resolve()
engine = scoring.make_scoring_engine(db, model, "tpcds")
print(engine.generate_sql(body)[-1])
```

## Generated SQL symptom

The initial `totals` CTE is correct: it aggregates all year-2000 returns by
returning customer and return-address state without a GA-home filter.

The planner then creates a virtual membership set containing only GA rows:

```sql
yummy AS (
    SELECT
        uneven.totals_cr_return_customer_id,
        uneven.totals_customer_total
    FROM uneven
    WHERE uneven.totals_home_state = 'GA'
)
```

It incorrectly applies that membership set while computing `state_avg`:

```sql
concerned AS (
    SELECT
        uneven.totals_return_state,
        avg(uneven.totals_customer_total) AS state_avg
    FROM uneven
    WHERE
        (uneven.totals_cr_return_customer_id,
         uneven.totals_customer_total) IN (
            SELECT ... FROM yummy
        )
    GROUP BY 1
)
```

The average is therefore over GA-home customers, not all customers in the
return-address state. The same membership restriction is appropriate for the
final output branch, but not for the independent average branch.

## Trigger matrix

| Shape | Result |
|---|---|
| `avg(totals.measure) by totals.partition`, no outer dimension filter | Correct all-row average |
| Add outer `totals.home_state = 'GA'` used only to filter output | GA membership is injected into the average |
| Compute equivalent average in raw SQL/independent CTE | Correct all-row average |
| Bind the source-year/state restriction inside the aggregate as in canonical q81 | Current workaround; correct result |

The essential trigger is a partitioned aggregate over a rowset plus an outer
filter on a sibling rowset field.

## Expected behavior

`state_avg` is defined from `totals` before the final query and is explicitly
partitioned only by `totals.return_state`. Its input population must be the
complete `totals` rowset.

The final `home_state = 'GA'` predicate should restrict only the rows compared
to and emitted alongside the already-computed state average. It must not be
routed into the average's source plan.

## Likely root cause

The generated `_virt_filter_*` membership shows that condition routing applies
the final rowset filter to every consumer of `totals`, including the independent
partitioned aggregate.

`trilogy/core/processing/discovery_utility.py` already contains a guard in
`get_loop_iteration_targets` for a `by`-partitioned aggregate injected only by
an outer filter. That guard drops outer conditions when the aggregate is not a
mandatory output. It does not cover this case, where `state_avg` is a required
dependency of the final comparison and the sibling GA filter is converted into
a virtual membership restriction before the average branch is sourced.

The likely fix locus is the condition-routing/filter-concept split around
`get_loop_iteration_targets`, plus the rowset filter handling that creates the
`_virt_filter_*` membership. A partitioned aggregate over a named rowset should
receive only predicates that are part of its own definition or constrain its
declared partition/input expression—not unrelated sibling predicates from a
consumer query.

## Regression test recommendation

Use a small rowset with columns `(entity, partition, segment, amount)`:

```text
entity  partition  segment  amount
1       A          keep     20
2       A          other    100
```

Define `partition_avg <- avg(amount) by partition`, then select only `segment =
'keep'` rows whose amount exceeds a threshold based on `partition_avg`.

Pin both contracts:

1. The average remains `60`, proving the consumer's segment filter did not leak
   into it.
2. Generated SQL for the average branch has no membership predicate derived
   from `segment = 'keep'`.

Also include a control where the segment filter is authored inside the rowset;
that version should intentionally produce an average of `20`.

## Resolution (2026-07-10)

Root cause was NOT discovery/condition routing for q81 — with
`CONFIG.optimizations.predicate_pushdown = False` the sourced plan was already
correct (the avg CTE had no membership). The leak was the `PredicatePushdown`
optimizer: `_check_parent` pushed the `_virt_filter_*` grain-key membership
(minted by the HAVING finer-dim semijoin rewrite) into the average's GROUP BY
CTE because the entity column was materialized there and every consumer carried
the atom. A scalar predicate lands in a grouping CTE as WHERE — pre-aggregation
— which is only sound when the predicate is constant within each group. The avg
groups by the partition (return state), not the entity, so the pushed WHERE
filtered rows inside groups and silently changed the average. Plain (non-
membership) HAVING atoms leaked identically.

Fix 1 — optimizer: `_predicate_safe_past_grouping` in
`trilogy/core/optimizations/predicate_pushdown.py`. A predicate may only push
into a `group_to_grain` CTE when every row argument is a group key, a pseudonym
of one, or a property keyed entirely by group keys — i.e. when it only drops
whole groups. This is unconditional (no provenance carve-outs): pushdown is an
optimization pass, so a push that changes what a group computes is a semantics
change, never a legal relocation. Applied in `_check_parent` and per-branch in
`_push_into_union_branches`.

Fix 2 — query44.preql: the guard exposed that TPC-DS q44's reference-matching
result (per-item average restricted to store 1) was being produced BY the
illegal optimizer push. The ruled language semantics
(`test_where_aggregate_input_not_filtered_by_where`, and the authored idiom in
query81.preql itself) are that an aggregate referenced in a WHERE is evaluated
over its own unfiltered scope; restricting its population must be authored
inside the aggregate. query44.preql now binds the filter:
`avg(ss.net_profit ? ss.store.sk = 1) by ss.item.sk`.

(A discovery-side "residual condition routing" variant — pushing the
non-self-referential WHERE atoms into the computed value's sourcing so
q44-as-authored stayed correct — was implemented and then REVERTED: it silently
narrowed WHERE-referenced sibling aggregates, breaking the ruled semantics
pinned by `test_where_aggregate_input_not_filtered_by_where` and reintroducing
this very bug through discovery for q30/q81. A note at the force-evaluation
site in `initialize_loop_context` records this.)

(TPC-DS q84 flipped fail→pass during this work, but that was a parallel
workstream's presence-probe fix landing in the same working tree, not this
change.)

Regression tests: `tests/test_pushdown_partitioned_aggregate_boundary.py` (q81
SQL + result contracts, authored-inside-aggregate control, and the q44 shape
correct with the optimizer disabled). Verified against the SF1 q81 database:
the minimal reproduction returns exactly the 367 reference rows (was 382).
