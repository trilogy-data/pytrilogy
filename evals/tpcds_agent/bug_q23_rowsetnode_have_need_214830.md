# q23 (run 20260629-214830, 1.55M tokens, FAILED) — REAL planner bug: "Have {RowsetNode...} and need" on filter-only rowset output vs named-concept RHS

Run: `evals/tpcds_agent/results/20260629-214830`, logs `agent_log.q23.{jsonl,conversation.txt}`,
final `workspace/query23.preql`.

## TL;DR

- **NEW framework planner bug** (distinct from the prior q23 handoffs, which only found the
  count-distinct footgun, the NULL-`max` footgun, and the now-FIXED groupby-aggregate /
  INVALID_REFERENCE bugs). Those reports' "agent thrash" verdict does **not** cover this run.
- The dominant error this run is a hard planner crash:
  `Syntax error: Have {'RowsetNode<customer_totals.cust_id,customer_totals.lifetime_total>': None}
  and need customer_totals.lifetime_total > local.threshold`
  raised at `trilogy/core/processing/concept_strategies_v3.py:265` (ValidationResult
  `INCOMPLETE_CONDITION`). Hit on `query23.preql`, `test_combined.preql`, `test_combined2.preql`
  (jsonl records 35, 98, 113).
- A second, separate framework error also appears (record 104):
  `Could not resolve connections for query with output
  ['local.frequent_desc_prefix<Purpose.PROPERTY>Derivation.FILTER>']` — a FILTER-derived **property**
  (`substring(item.desc,1,30) ? (count_distinct(...) > 4)`) used as a membership RHS. Reproduced and
  isolated, but secondary.

## Symptom / failing construct

The agent's natural formulation of "best customers" — a rowset that selects ONE column of a prior
grouped rowset and filters on a SIBLING column against a derived threshold:

```preql
import raw.store_sales as store;
with customer_totals as
where store.date.year >= 2000 and store.customer.id is not null
select store.customer.id as cust_id,
       sum(store.quantity * store.sales_price) as lifetime_total;

auto threshold <- 0.5 * (max(customer_totals.lifetime_total) by *);

with best_customers as
select customer_totals.cust_id          -- lifetime_total NOT selected
where customer_totals.lifetime_total > threshold;   -- filter-only + named RHS

select best_customers.cust_id;
```

→ `SyntaxError: Have {'RowsetNode<customer_totals.cust_id,customer_totals.lifetime_total>': None}
and need customer_totals.lifetime_total > local.threshold`

Reproduced via `generate_sql` against `.../workspace/_worker_0` (the eval DB). The canonical
`tests/modeling/tpc_ds_duckdb/query23.preql` builds clean — it dodges the trigger two ways: it uses
**HAVING** (not WHERE) and the filtered LHS is a base-bound aggregate concept
(`customer_total_overall`), **not** a rowset output column.

## Trigger matrix (all rows share the `customer_totals` rowset above)

| # | consuming form | filter | RHS | result |
|---|---|---|---|---|
| A | `with best as select customer_totals.cust_id` | `WHERE lifetime_total > threshold` | named concept | **FAIL Have/need** |
| F | top-level `select customer_totals.cust_id` | `WHERE lifetime_total > threshold` | named concept | **FAIL Have/need** |
| H | top-level select cust_id | `WHERE lifetime_total > threshold` | `threshold <- 500*2` (non-aggregate) | **FAIL Have/need** |
| I | top-level select cust_id | `WHERE lifetime_total > threshold` | `max(store.quantity) by *` | **FAIL Have/need** |
| J | top-level select cust_id | `WHERE lifetime_total > threshold` | `max(lifetime_total) by *` | **FAIL Have/need** |
| C | top-level select cust_id | `WHERE lifetime_total > 1000` | **literal** | OK |
| B | `select cust_id, lifetime_total` | `WHERE lifetime_total > threshold` | named concept | OK |
| G | `select lifetime_total` | `WHERE lifetime_total > threshold` | named concept | OK |
| D | `select cust_id` | **`HAVING` lifetime_total > threshold** | named concept | OK |
| E | NO inner rowset; base-bound `auto cust_total` | `WHERE cust_total > threshold` | named concept | OK |

**Necessary-and-sufficient trigger:** (1) the filtered column is a **rowset output**, AND (2) it is
referenced **only in the WHERE** (not in the consuming SELECT's outputs), AND (3) the comparison RHS
is a **named concept** (literal RHS works — C). Aggregate-ness of the RHS is irrelevant (H, a plain
`500*2`, still fails). HAVING (D), selecting the filtered column (B/G), or a base-bound LHS (E) all
avoid it.

## Root cause

The WHERE atom `customer_totals.lifetime_total > local.threshold` has **two** `row_arguments`:
the rowset output and the threshold concept. With a literal RHS (C) there is only one row_argument
(the rowset output, which the RowsetNode provides), so it validates.

- Emission: `concept_strategies_v3.py:265` raises on `ValidationResult.INCOMPLETE_CONDITION`.
- Decision: `trilogy/core/processing/discovery_validation.py`
  - `_conditions_met` (line 85) → `_condition_atom_met` (line 70): first clause needs **all**
    row_arguments in `found_addresses`. `lifetime_total` is found (RowsetNode emits it) but
    `local.threshold` is **not on this stack** (it's a single-row scalar to be cross-joined later),
    so the clause fails.
  - The fallback (lines 77-82) requires every stack node to be `_is_scalar_only` /
    `_is_independent_scope` / `_node_condition_implies`. `_is_independent_scope` (line 38) **correctly
    refuses** to exempt the RowsetNode because the condition constrains one of its visible outputs
    (`lifetime_total`); the node's `preexisting_conditions is None`. → fallback fails → atom not met →
    INCOMPLETE_CONDITION.
- Why the threshold is never co-located: `initialize_loop_context`
  (`concept_strategies_v3.py:170-192`) only sets
  `must_evaluate_condition_on_this_level_not_push_down` when a concept **in `mandatory_list`** (the
  query outputs) is a derived filter input. Here the filtered `lifetime_total` is **filter-only**, so
  it is never in `mandatory_list`; the flag stays False and the planner never forces a level that
  joins the `threshold` scalar in alongside the RowsetNode before validating the WHERE. The condition
  is thus structurally unsatisfiable at the only level that has the rowset output → deadlock.

So: **a filter-only rowset-output column compared to a non-literal concept is never co-sourced with
that concept, so its WHERE can never be applied** — the planner crashes instead of either cross-joining
the scalar at the rowset level or pushing the filter onto the rowset.

## Classification

**REAL framework bug** (planner / source-resolution, condition co-sourcing). It is a hard crash on an
idiomatic construct, not a footgun, not agent thrash. It is **new** relative to the two prior q23
handoffs (`bug_q23_residual_churn.md`, `bug_q23_churn_013151.md`), whose "no new bug / agent thrash"
verdicts predate this run and did not exercise the rowset-of-rowset filter-only WHERE path. The
workarounds (HAVING, also-select the filtered column, or a base-bound aggregate concept instead of a
rowset output) are non-obvious, which is what drove the 1.55M-token churn.

### Secondary (separate) bug
`auto frequent_desc_prefix <- substring(store.item.desc,1,30) ? (count_distinct(...) > 4)` used as a
membership RHS → `UnresolvableQueryException: Could not resolve connections for query with output
['local.frequent_desc_prefix<Purpose.PROPERTY>Derivation.FILTER>']`. Isolated: the `best_cust_id`
membership alone resolves OK; only the FILTER-over-derived-property set fails. Cryptic, but distinct
from the Have/need bug. Canonical models frequent-items as a rowset, avoiding it.

## Repro

`generate_sql` harness against `evals/tpcds_agent/results/20260629-214830/workspace/_worker_0`
(`tpcds.duckdb`, namespace `tpcds`). Minimal body = the A snippet above.
